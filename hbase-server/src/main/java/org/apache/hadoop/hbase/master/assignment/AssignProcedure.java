/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.master.assignment;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.assignment.RegionStates.RegionStateNode;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher.RegionOpenOperation;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AssignRegionStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionTransitionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

/**
 * Procedure that describe the assignment of a single region.
 * There can only be one RegionTransitionProcedure per region running at a time
 * since each procedure takes a lock on the region.
 *
 * <p>The Assign starts by pushing the "assign" operation to the AssignmentManager
 * and then will go in a "waiting" state.
 * The AM will batch the "assign" requests and ask the Balancer where to put
 * the region (the various policies will be respected: retain, round-robin, random).
 * Once the AM and the balancer have found a place for the region the procedure
 * will be resumed and an "open region" request will be placed in the Remote Dispatcher
 * queue, and the procedure once again will go in a "waiting state".
 * The Remote Dispatcher will batch the various requests for that server and
 * they will be sent to the RS for execution.
 * The RS will complete the open operation by calling master.reportRegionStateTransition().
 * The AM will intercept the transition report, and notify the procedure.
 * The procedure will finish the assignment by publishing to new state on meta
 * or it will retry the assignment.
 *
 * <p>This procedure does not rollback when beyond the first
 * REGION_TRANSITION_QUEUE step; it will press on trying to assign in the face of
 * failure. Should we ignore rollback calls to Assign/Unassign then? Or just
 * remove rollback here?
 */
@InterfaceAudience.Private
public class AssignProcedure extends RegionTransitionProcedure {
  private static final Log LOG = LogFactory.getLog(AssignProcedure.class);

  private boolean forceNewPlan = false;

  public AssignProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    super();
  }

  public AssignProcedure(final HRegionInfo regionInfo) {
    this(regionInfo, false);
  }

  public AssignProcedure(final HRegionInfo regionInfo, final boolean forceNewPlan) {
    super(regionInfo);
    this.forceNewPlan = forceNewPlan;
    this.server = null;
  }

  public AssignProcedure(final HRegionInfo regionInfo, final ServerName destinationServer) {
    super(regionInfo);
    this.forceNewPlan = false;
    this.server = destinationServer;
  }

  public ServerName getServer() {
    return this.server;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.ASSIGN;
  }

  @Override
  protected boolean isRollbackSupported(final RegionTransitionState state) {
    switch (state) {
      case REGION_TRANSITION_QUEUE:
        return true;
      default:
        return false;
    }
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    final AssignRegionStateData.Builder state = AssignRegionStateData.newBuilder()
        .setTransitionState(getTransitionState())
        .setRegionInfo(HRegionInfo.convert(getRegionInfo()));
    if (forceNewPlan) {
      state.setForceNewPlan(true);
    }
    if (server != null) {
      state.setTargetServer(ProtobufUtil.toServerName(server));
    }
    state.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    final AssignRegionStateData state = AssignRegionStateData.parseDelimitedFrom(stream);
    setTransitionState(state.getTransitionState());
    setRegionInfo(HRegionInfo.convert(state.getRegionInfo()));
    forceNewPlan = state.getForceNewPlan();
    if (state.hasTargetServer()) {
      server = ProtobufUtil.toServerName(state.getTargetServer());
    }
  }

  @Override
  protected boolean startTransition(final MasterProcedureEnv env, final RegionStateNode regionNode)
      throws IOException {
    // If the region is already open we can't do much...
    if (regionNode.isInState(State.OPEN) && isServerOnline(env, regionNode)) {
      LOG.info("Assigned, not reassigning; " + this + "; " + regionNode.toShortString());
      return false;
    }
    // If the region is SPLIT, we can't assign it.
    if (regionNode.isInState(State.SPLIT)) {
      LOG.info("SPLIT, cannot be assigned; " +
          this + "; " + regionNode.toShortString());
      return false;
    }

    // If we haven't started the operation yet, we can abort
    if (aborted.get() && regionNode.isInState(State.CLOSED, State.OFFLINE)) {
      if (incrementAndCheckMaxAttempts(env, regionNode)) {
        regionNode.setState(State.FAILED_OPEN);
        setFailure(getClass().getSimpleName(),
          new RetriesExhaustedException("Max attempts exceeded"));
      } else {
        setAbortFailure(getClass().getSimpleName(), "Abort requested");
      }
      return false;
    }

    // send assign (add into assign-pool). region is now in OFFLINE state
    ServerName lastRegionLocation = regionNode.offline();
    boolean retain = false;
    if (!forceNewPlan) {
      if (this.server != null) {
        regionNode.setRegionLocation(server);
      } else {
        // Try to 'retain' old assignment.
        retain = true;
        if (lastRegionLocation != null) regionNode.setRegionLocation(lastRegionLocation);
      }
    }
    LOG.info("Start " + this + "; " + regionNode.toShortString() +
        "; forceNewPlan=" + this.forceNewPlan +
        ", retain=" + retain);
    env.getAssignmentManager().queueAssign(regionNode);
    return true;
  }

  @Override
  protected boolean updateTransition(final MasterProcedureEnv env, final RegionStateNode regionNode)
  throws IOException, ProcedureSuspendedException {
    // TODO: crash if destinationServer is specified and not online
    // which is also the case when the balancer provided us with a different location.
    if (LOG.isTraceEnabled()) {
      LOG.trace("Update " + this + "; " + regionNode.toShortString());
    }
    if (regionNode.getRegionLocation() == null) {
      setTransitionState(RegionTransitionState.REGION_TRANSITION_QUEUE);
      return true;
    } else if (this.server == null) {
      // Update our server reference to align with regionNode so toString
      // aligns with what regionNode has.
      this.server = regionNode.getRegionLocation();
    }

    if (!isServerOnline(env, regionNode)) {
      // TODO: is this correct? should we wait the chore/ssh?
      LOG.info("Server not online: " + this + "; " + regionNode.toShortString());
      setTransitionState(RegionTransitionState.REGION_TRANSITION_QUEUE);
      return true;
    }

    // Wait until server reported. If we have resumed the region may already be assigned.
    if (LOG.isTraceEnabled()) {
      LOG.trace("Wait report on " +
          this /*Full detail on this procedure -- includes server name*/);
    }
    if (env.getAssignmentManager().waitServerReportEvent(regionNode.getRegionLocation(), this)) {
      LOG.info("Early suspend! " + this + "; " + regionNode.toShortString());
      throw new ProcedureSuspendedException();
    }

    if (regionNode.isInState(State.OPEN)) {
      LOG.info("Already assigned: " + this + "; " + regionNode.toShortString());
      return false;
    }

    // Set OPENING in hbase:meta and add region to list of regions on server.
    env.getAssignmentManager().markRegionAsOpening(regionNode);

    // TODO: Requires a migration to be open by the RS?
    // regionNode.getFormatVersion()

    addToRemoteDispatcher(env, regionNode.getRegionLocation());
    // We always return true, even if we fail dispatch because failiure sets
    // state back to beginning so we retry assign.
    return true;
  }

  @Override
  protected void finishTransition(final MasterProcedureEnv env, final RegionStateNode regionNode)
      throws IOException {
    env.getAssignmentManager().markRegionAsOpened(regionNode);
    // This success may have been after we failed open a few times. Be sure to cleanup any
    // failed open references. See #incrementAndCheckMaxAttempts and where it is called.
    env.getAssignmentManager().getRegionStates().removeFromFailedOpen(regionNode.getRegionInfo());
  }

  @Override
  protected void reportTransition(final MasterProcedureEnv env, final RegionStateNode regionNode,
      final TransitionCode code, final long openSeqNum) throws UnexpectedStateException {
    switch (code) {
      case OPENED:
        if (openSeqNum < 0) {
          throw new UnexpectedStateException("Received report unexpected " + code +
              " transition openSeqNum=" + openSeqNum + ", " + regionNode);
        }
        if (openSeqNum < regionNode.getOpenSeqNum()) {
          LOG.warn("Skipping update of open seqnum with " + openSeqNum +
              " because current seqnum=" + regionNode.getOpenSeqNum());
        }
        regionNode.setOpenSeqNum(openSeqNum);
        // Leave the state here as OPENING for now. We set it to OPEN in
        // REGION_TRANSITION_FINISH section where we do a bunch of checks.
        // regionNode.setState(RegionState.State.OPEN, RegionState.State.OPENING);
        setTransitionState(RegionTransitionState.REGION_TRANSITION_FINISH);
        break;
      case FAILED_OPEN:
        handleFailure(env, regionNode);
        break;
      default:
        throw new UnexpectedStateException("Received report unexpected " + code +
            " transition openSeqNum=" + openSeqNum + ", " + regionNode.toShortString() +
            ", " + this + ", expected OPENED or FAILED_OPEN.");
    }
  }

  private void handleFailure(final MasterProcedureEnv env, final RegionStateNode regionNode) {
    if (incrementAndCheckMaxAttempts(env, regionNode)) {
      aborted.set(true);
    }
    this.forceNewPlan = true;
    this.server = null;
    regionNode.offline();
    env.getAssignmentManager().undoRegionAsOpening(regionNode);
    setTransitionState(RegionTransitionState.REGION_TRANSITION_QUEUE);
  }

  private boolean incrementAndCheckMaxAttempts(final MasterProcedureEnv env,
      final RegionStateNode regionNode) {
    final int retries = env.getAssignmentManager().getRegionStates().
        addToFailedOpen(regionNode).incrementAndGetRetries();
    int max = env.getAssignmentManager().getAssignMaxAttempts();
    LOG.info("Retry=" + retries + " of max=" + max + "; " +
        this + "; " + regionNode.toShortString());
    return retries >= max;
  }

  @Override
  public RemoteOperation remoteCallBuild(final MasterProcedureEnv env, final ServerName serverName) {
    assert serverName.equals(getRegionState(env).getRegionLocation());
    return new RegionOpenOperation(this, getRegionInfo(),
        env.getAssignmentManager().getFavoredNodes(getRegionInfo()), false);
  }

  @Override
  protected void remoteCallFailed(final MasterProcedureEnv env, final RegionStateNode regionNode,
      final IOException exception) {
    handleFailure(env, regionNode);
  }
}