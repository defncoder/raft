// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: raftrpc.proto

package raft.rpc;

public final class RaftContainer {
  private RaftContainer() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_rpc_LogEntry_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_rpc_LogEntry_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_rpc_AppendRequest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_rpc_AppendRequest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_rpc_AppendResponse_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_rpc_AppendResponse_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_rpc_VoteRequest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_rpc_VoteRequest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_rpc_VoteResponse_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_rpc_VoteResponse_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\rraftrpc.proto\022\003rpc\"C\n\010LogEntry\022\021\n\tlog_" +
      "index\030\001 \001(\003\022\023\n\013term_number\030\002 \001(\003\022\017\n\007comm" +
      "and\030\003 \001(\t\"\226\001\n\rAppendRequest\022\014\n\004term\030\001 \001(" +
      "\003\022\020\n\010leaderId\030\002 \001(\t\022\024\n\014prevLogIndex\030\003 \001(" +
      "\003\022\023\n\013prevLogTerm\030\004 \001(\003\022\031\n\021leaderCommitIn" +
      "dex\030\005 \001(\003\022\037\n\010logEntry\030\006 \003(\0132\r.rpc.LogEnt" +
      "ry\"/\n\016AppendResponse\022\014\n\004term\030\001 \001(\003\022\017\n\007su" +
      "ccess\030\002 \001(\010\"[\n\013VoteRequest\022\014\n\004term\030\001 \001(\003" +
      "\022\023\n\013candidateId\030\002 \001(\t\022\024\n\014lastLogIndex\030\003 " +
      "\001(\003\022\023\n\013lastLogTerm\030\004 \001(\003\"1\n\014VoteResponse" +
      "\022\014\n\004term\030\001 \001(\003\022\023\n\013voteGranted\030\002 \001(\0102{\n\007R" +
      "aftRPC\022:\n\rAppendEntries\022\022.rpc.AppendRequ" +
      "est\032\023.rpc.AppendResponse\"\000\0224\n\013RequestVot" +
      "e\022\020.rpc.VoteRequest\032\021.rpc.VoteResponse\"\000" +
      "B\"\n\010raft.rpcB\rRaftContainerP\001\242\002\004RRPCb\006pr" +
      "oto3"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
        new com.google.protobuf.Descriptors.FileDescriptor.    InternalDescriptorAssigner() {
          public com.google.protobuf.ExtensionRegistry assignDescriptors(
              com.google.protobuf.Descriptors.FileDescriptor root) {
            descriptor = root;
            return null;
          }
        };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
    internal_static_rpc_LogEntry_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_rpc_LogEntry_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_rpc_LogEntry_descriptor,
        new java.lang.String[] { "LogIndex", "TermNumber", "Command", });
    internal_static_rpc_AppendRequest_descriptor =
      getDescriptor().getMessageTypes().get(1);
    internal_static_rpc_AppendRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_rpc_AppendRequest_descriptor,
        new java.lang.String[] { "Term", "LeaderId", "PrevLogIndex", "PrevLogTerm", "LeaderCommitIndex", "LogEntry", });
    internal_static_rpc_AppendResponse_descriptor =
      getDescriptor().getMessageTypes().get(2);
    internal_static_rpc_AppendResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_rpc_AppendResponse_descriptor,
        new java.lang.String[] { "Term", "Success", });
    internal_static_rpc_VoteRequest_descriptor =
      getDescriptor().getMessageTypes().get(3);
    internal_static_rpc_VoteRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_rpc_VoteRequest_descriptor,
        new java.lang.String[] { "Term", "CandidateId", "LastLogIndex", "LastLogTerm", });
    internal_static_rpc_VoteResponse_descriptor =
      getDescriptor().getMessageTypes().get(4);
    internal_static_rpc_VoteResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_rpc_VoteResponse_descriptor,
        new java.lang.String[] { "Term", "VoteGranted", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
