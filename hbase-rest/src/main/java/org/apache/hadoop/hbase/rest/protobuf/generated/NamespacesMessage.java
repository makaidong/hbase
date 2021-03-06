// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: NamespacesMessage.proto

package org.apache.hadoop.hbase.rest.protobuf.generated;

public final class NamespacesMessage {
  private NamespacesMessage() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface NamespacesOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // repeated string namespace = 1;
    /**
     * <code>repeated string namespace = 1;</code>
     */
    java.util.List<java.lang.String>
    getNamespaceList();
    /**
     * <code>repeated string namespace = 1;</code>
     */
    int getNamespaceCount();
    /**
     * <code>repeated string namespace = 1;</code>
     */
    java.lang.String getNamespace(int index);
    /**
     * <code>repeated string namespace = 1;</code>
     */
    com.google.protobuf.ByteString
        getNamespaceBytes(int index);
  }
  /**
   * Protobuf type {@code org.apache.hadoop.hbase.rest.protobuf.generated.Namespaces}
   */
  public static final class Namespaces extends
      com.google.protobuf.GeneratedMessage
      implements NamespacesOrBuilder {
    // Use Namespaces.newBuilder() to construct.
    private Namespaces(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private Namespaces(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final Namespaces defaultInstance;
    public static Namespaces getDefaultInstance() {
      return defaultInstance;
    }

    public Namespaces getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private Namespaces(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              if (!((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
                namespace_ = new com.google.protobuf.LazyStringArrayList();
                mutable_bitField0_ |= 0x00000001;
              }
              namespace_.add(input.readBytes());
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        if (((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
          namespace_ = new com.google.protobuf.UnmodifiableLazyStringList(namespace_);
        }
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.internal_static_org_apache_hadoop_hbase_rest_protobuf_generated_Namespaces_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.internal_static_org_apache_hadoop_hbase_rest_protobuf_generated_Namespaces_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces.class, org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces.Builder.class);
    }

    public static com.google.protobuf.Parser<Namespaces> PARSER =
        new com.google.protobuf.AbstractParser<Namespaces>() {
      public Namespaces parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new Namespaces(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<Namespaces> getParserForType() {
      return PARSER;
    }

    // repeated string namespace = 1;
    public static final int NAMESPACE_FIELD_NUMBER = 1;
    private com.google.protobuf.LazyStringList namespace_;
    /**
     * <code>repeated string namespace = 1;</code>
     */
    public java.util.List<java.lang.String>
        getNamespaceList() {
      return namespace_;
    }
    /**
     * <code>repeated string namespace = 1;</code>
     */
    public int getNamespaceCount() {
      return namespace_.size();
    }
    /**
     * <code>repeated string namespace = 1;</code>
     */
    public java.lang.String getNamespace(int index) {
      return namespace_.get(index);
    }
    /**
     * <code>repeated string namespace = 1;</code>
     */
    public com.google.protobuf.ByteString
        getNamespaceBytes(int index) {
      return namespace_.getByteString(index);
    }

    private void initFields() {
      namespace_ = com.google.protobuf.LazyStringArrayList.EMPTY;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      for (int i = 0; i < namespace_.size(); i++) {
        output.writeBytes(1, namespace_.getByteString(i));
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      {
        int dataSize = 0;
        for (int i = 0; i < namespace_.size(); i++) {
          dataSize += com.google.protobuf.CodedOutputStream
            .computeBytesSizeNoTag(namespace_.getByteString(i));
        }
        size += dataSize;
        size += 1 * getNamespaceList().size();
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code org.apache.hadoop.hbase.rest.protobuf.generated.Namespaces}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.NamespacesOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.internal_static_org_apache_hadoop_hbase_rest_protobuf_generated_Namespaces_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.internal_static_org_apache_hadoop_hbase_rest_protobuf_generated_Namespaces_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces.class, org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces.Builder.class);
      }

      // Construct using org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        namespace_ = com.google.protobuf.LazyStringArrayList.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.internal_static_org_apache_hadoop_hbase_rest_protobuf_generated_Namespaces_descriptor;
      }

      public org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces getDefaultInstanceForType() {
        return org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces.getDefaultInstance();
      }

      public org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces build() {
        org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces buildPartial() {
        org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces result = new org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces(this);
        int from_bitField0_ = bitField0_;
        if (((bitField0_ & 0x00000001) == 0x00000001)) {
          namespace_ = new com.google.protobuf.UnmodifiableLazyStringList(
              namespace_);
          bitField0_ = (bitField0_ & ~0x00000001);
        }
        result.namespace_ = namespace_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces) {
          return mergeFrom((org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces other) {
        if (other == org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces.getDefaultInstance()) return this;
        if (!other.namespace_.isEmpty()) {
          if (namespace_.isEmpty()) {
            namespace_ = other.namespace_;
            bitField0_ = (bitField0_ & ~0x00000001);
          } else {
            ensureNamespaceIsMutable();
            namespace_.addAll(other.namespace_);
          }
          onChanged();
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // repeated string namespace = 1;
      private com.google.protobuf.LazyStringList namespace_ = com.google.protobuf.LazyStringArrayList.EMPTY;
      private void ensureNamespaceIsMutable() {
        if (!((bitField0_ & 0x00000001) == 0x00000001)) {
          namespace_ = new com.google.protobuf.LazyStringArrayList(namespace_);
          bitField0_ |= 0x00000001;
         }
      }
      /**
       * <code>repeated string namespace = 1;</code>
       */
      public java.util.List<java.lang.String>
          getNamespaceList() {
        return java.util.Collections.unmodifiableList(namespace_);
      }
      /**
       * <code>repeated string namespace = 1;</code>
       */
      public int getNamespaceCount() {
        return namespace_.size();
      }
      /**
       * <code>repeated string namespace = 1;</code>
       */
      public java.lang.String getNamespace(int index) {
        return namespace_.get(index);
      }
      /**
       * <code>repeated string namespace = 1;</code>
       */
      public com.google.protobuf.ByteString
          getNamespaceBytes(int index) {
        return namespace_.getByteString(index);
      }
      /**
       * <code>repeated string namespace = 1;</code>
       */
      public Builder setNamespace(
          int index, java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  ensureNamespaceIsMutable();
        namespace_.set(index, value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated string namespace = 1;</code>
       */
      public Builder addNamespace(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  ensureNamespaceIsMutable();
        namespace_.add(value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated string namespace = 1;</code>
       */
      public Builder addAllNamespace(
          java.lang.Iterable<java.lang.String> values) {
        ensureNamespaceIsMutable();
        super.addAll(values, namespace_);
        onChanged();
        return this;
      }
      /**
       * <code>repeated string namespace = 1;</code>
       */
      public Builder clearNamespace() {
        namespace_ = com.google.protobuf.LazyStringArrayList.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000001);
        onChanged();
        return this;
      }
      /**
       * <code>repeated string namespace = 1;</code>
       */
      public Builder addNamespaceBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  ensureNamespaceIsMutable();
        namespace_.add(value);
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:org.apache.hadoop.hbase.rest.protobuf.generated.Namespaces)
    }

    static {
      defaultInstance = new Namespaces(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:org.apache.hadoop.hbase.rest.protobuf.generated.Namespaces)
  }

  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_org_apache_hadoop_hbase_rest_protobuf_generated_Namespaces_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_org_apache_hadoop_hbase_rest_protobuf_generated_Namespaces_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\027NamespacesMessage.proto\022/org.apache.ha" +
      "doop.hbase.rest.protobuf.generated\"\037\n\nNa" +
      "mespaces\022\021\n\tnamespace\030\001 \003(\t"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_org_apache_hadoop_hbase_rest_protobuf_generated_Namespaces_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_org_apache_hadoop_hbase_rest_protobuf_generated_Namespaces_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_org_apache_hadoop_hbase_rest_protobuf_generated_Namespaces_descriptor,
              new java.lang.String[] { "Namespace", });
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}
