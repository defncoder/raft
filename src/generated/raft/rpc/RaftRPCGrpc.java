package raft.rpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 * <pre>
 * Interface exported by the server.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.26.0)",
    comments = "Source: raftrpc.proto")
public final class RaftRPCGrpc {

  private RaftRPCGrpc() {}

  public static final String SERVICE_NAME = "rpc.RaftRPC";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<raft.rpc.AppendRequest,
      raft.rpc.AppendResponse> getAppendEntriesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AppendEntries",
      requestType = raft.rpc.AppendRequest.class,
      responseType = raft.rpc.AppendResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<raft.rpc.AppendRequest,
      raft.rpc.AppendResponse> getAppendEntriesMethod() {
    io.grpc.MethodDescriptor<raft.rpc.AppendRequest, raft.rpc.AppendResponse> getAppendEntriesMethod;
    if ((getAppendEntriesMethod = RaftRPCGrpc.getAppendEntriesMethod) == null) {
      synchronized (RaftRPCGrpc.class) {
        if ((getAppendEntriesMethod = RaftRPCGrpc.getAppendEntriesMethod) == null) {
          RaftRPCGrpc.getAppendEntriesMethod = getAppendEntriesMethod =
              io.grpc.MethodDescriptor.<raft.rpc.AppendRequest, raft.rpc.AppendResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AppendEntries"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  raft.rpc.AppendRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  raft.rpc.AppendResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RaftRPCMethodDescriptorSupplier("AppendEntries"))
              .build();
        }
      }
    }
    return getAppendEntriesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<raft.rpc.VoteRequest,
      raft.rpc.VoteResponse> getRequestVoteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RequestVote",
      requestType = raft.rpc.VoteRequest.class,
      responseType = raft.rpc.VoteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<raft.rpc.VoteRequest,
      raft.rpc.VoteResponse> getRequestVoteMethod() {
    io.grpc.MethodDescriptor<raft.rpc.VoteRequest, raft.rpc.VoteResponse> getRequestVoteMethod;
    if ((getRequestVoteMethod = RaftRPCGrpc.getRequestVoteMethod) == null) {
      synchronized (RaftRPCGrpc.class) {
        if ((getRequestVoteMethod = RaftRPCGrpc.getRequestVoteMethod) == null) {
          RaftRPCGrpc.getRequestVoteMethod = getRequestVoteMethod =
              io.grpc.MethodDescriptor.<raft.rpc.VoteRequest, raft.rpc.VoteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RequestVote"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  raft.rpc.VoteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  raft.rpc.VoteResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RaftRPCMethodDescriptorSupplier("RequestVote"))
              .build();
        }
      }
    }
    return getRequestVoteMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static RaftRPCStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RaftRPCStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RaftRPCStub>() {
        @java.lang.Override
        public RaftRPCStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RaftRPCStub(channel, callOptions);
        }
      };
    return RaftRPCStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static RaftRPCBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RaftRPCBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RaftRPCBlockingStub>() {
        @java.lang.Override
        public RaftRPCBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RaftRPCBlockingStub(channel, callOptions);
        }
      };
    return RaftRPCBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static RaftRPCFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RaftRPCFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RaftRPCFutureStub>() {
        @java.lang.Override
        public RaftRPCFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RaftRPCFutureStub(channel, callOptions);
        }
      };
    return RaftRPCFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * Interface exported by the server.
   * </pre>
   */
  public static abstract class RaftRPCImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Append log entries
     * Sent from leader to followers to append a list of log entries from leader to followers.
     * AppendResponse is returned.
     * </pre>
     */
    public void appendEntries(raft.rpc.AppendRequest request,
        io.grpc.stub.StreamObserver<raft.rpc.AppendResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getAppendEntriesMethod(), responseObserver);
    }

    /**
     * <pre>
     * Request for vote
     * Sent from a candidate to all other machines requesting a vote for candidate.
     * VoteResponse is returned.
     * </pre>
     */
    public void requestVote(raft.rpc.VoteRequest request,
        io.grpc.stub.StreamObserver<raft.rpc.VoteResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRequestVoteMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getAppendEntriesMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                raft.rpc.AppendRequest,
                raft.rpc.AppendResponse>(
                  this, METHODID_APPEND_ENTRIES)))
          .addMethod(
            getRequestVoteMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                raft.rpc.VoteRequest,
                raft.rpc.VoteResponse>(
                  this, METHODID_REQUEST_VOTE)))
          .build();
    }
  }

  /**
   * <pre>
   * Interface exported by the server.
   * </pre>
   */
  public static final class RaftRPCStub extends io.grpc.stub.AbstractAsyncStub<RaftRPCStub> {
    private RaftRPCStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RaftRPCStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RaftRPCStub(channel, callOptions);
    }

    /**
     * <pre>
     * Append log entries
     * Sent from leader to followers to append a list of log entries from leader to followers.
     * AppendResponse is returned.
     * </pre>
     */
    public void appendEntries(raft.rpc.AppendRequest request,
        io.grpc.stub.StreamObserver<raft.rpc.AppendResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getAppendEntriesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Request for vote
     * Sent from a candidate to all other machines requesting a vote for candidate.
     * VoteResponse is returned.
     * </pre>
     */
    public void requestVote(raft.rpc.VoteRequest request,
        io.grpc.stub.StreamObserver<raft.rpc.VoteResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRequestVoteMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * Interface exported by the server.
   * </pre>
   */
  public static final class RaftRPCBlockingStub extends io.grpc.stub.AbstractBlockingStub<RaftRPCBlockingStub> {
    private RaftRPCBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RaftRPCBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RaftRPCBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Append log entries
     * Sent from leader to followers to append a list of log entries from leader to followers.
     * AppendResponse is returned.
     * </pre>
     */
    public raft.rpc.AppendResponse appendEntries(raft.rpc.AppendRequest request) {
      return blockingUnaryCall(
          getChannel(), getAppendEntriesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Request for vote
     * Sent from a candidate to all other machines requesting a vote for candidate.
     * VoteResponse is returned.
     * </pre>
     */
    public raft.rpc.VoteResponse requestVote(raft.rpc.VoteRequest request) {
      return blockingUnaryCall(
          getChannel(), getRequestVoteMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * Interface exported by the server.
   * </pre>
   */
  public static final class RaftRPCFutureStub extends io.grpc.stub.AbstractFutureStub<RaftRPCFutureStub> {
    private RaftRPCFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RaftRPCFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RaftRPCFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Append log entries
     * Sent from leader to followers to append a list of log entries from leader to followers.
     * AppendResponse is returned.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<raft.rpc.AppendResponse> appendEntries(
        raft.rpc.AppendRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getAppendEntriesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Request for vote
     * Sent from a candidate to all other machines requesting a vote for candidate.
     * VoteResponse is returned.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<raft.rpc.VoteResponse> requestVote(
        raft.rpc.VoteRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRequestVoteMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_APPEND_ENTRIES = 0;
  private static final int METHODID_REQUEST_VOTE = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final RaftRPCImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(RaftRPCImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_APPEND_ENTRIES:
          serviceImpl.appendEntries((raft.rpc.AppendRequest) request,
              (io.grpc.stub.StreamObserver<raft.rpc.AppendResponse>) responseObserver);
          break;
        case METHODID_REQUEST_VOTE:
          serviceImpl.requestVote((raft.rpc.VoteRequest) request,
              (io.grpc.stub.StreamObserver<raft.rpc.VoteResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class RaftRPCBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    RaftRPCBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return raft.rpc.RaftContainer.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("RaftRPC");
    }
  }

  private static final class RaftRPCFileDescriptorSupplier
      extends RaftRPCBaseDescriptorSupplier {
    RaftRPCFileDescriptorSupplier() {}
  }

  private static final class RaftRPCMethodDescriptorSupplier
      extends RaftRPCBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    RaftRPCMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (RaftRPCGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new RaftRPCFileDescriptorSupplier())
              .addMethod(getAppendEntriesMethod())
              .addMethod(getRequestVoteMethod())
              .build();
        }
      }
    }
    return result;
  }
}
