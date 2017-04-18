package cn.ac.ict.msbench.ms;

import cn.ac.ict.msbench.MS;
import cn.ac.ict.msbench.exception.MSException;
import cn.ac.ict.msbench.worker.callback.ReadCallBack;
import cn.ac.ict.msbench.worker.callback.WriteCallBack;

import com.google.common.base.Preconditions;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.distributedlog.*;
import com.twitter.distributedlog.client.serverset.DLZkServerSet;
import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.distributedlog.service.DistributedLogClientBuilder;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.util.Duration$;
import com.twitter.util.FutureEventListener;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by jiecxy on 2017/3/15.
 */
public class DLClient extends MS {
    private static final String SERVERSETPATHS = "serversetpaths";
    private static final String FINAGLENAMES = "finagleNames";
    private static final String DLURI = "uri";
    private static final String HOSTCONNECTIONLIMIT = "hostConnectionLimit";
    private static final String HOSTCONNECTIONCORESIZE = "hostConnectionCoresize";
    private static final String THRIFTMUX = "thriftmux";
    private static final String READBULKNUM = "readbulknum";
    private static final String ISWRITER ="iswriter";

    DistributedLogConfiguration conf = null;
    AsyncLogReader reader = null;
    AsyncLogWriter writer = null;
    LogWriter syncwriter = null;
    ClientBuilder clientBuilder = null;
    DistributedLogClientBuilder builder = null;
    DistributedLogClient client = null;
    DistributedLogNamespace namespace = null;
    DLZkServerSet[] serverSets = null;
    //参数
    URI uri = null;
    int readbulknum = 10;
    int hostConnectionLimit = 1;
    int hostConnectionCoresize = 1;
    boolean thriftmux = true;
    List<String> serversetPaths = new ArrayList<String>();
    List<String> finagleNames = new ArrayList<String>();
    boolean iswriter = true;

    public DLClient(String streamName, boolean isProducer, Properties p,int from) {
        super(streamName, isProducer, p,from);
        uri = URI.create((String) p.remove(DLURI));
        Preconditions.checkNotNull(uri);
        conf = new DistributedLogConfiguration()
                .setLogSegmentRollingIntervalMinutes(60) // interval to roll log segment
                .setRetentionPeriodHours(1) // retention period
                .setWriteQuorumSize(2) // 2 replicas
                .setAckQuorumSize(2) // 2 replicas
                .setEnsembleSize(3);
        conf.setImmediateFlushEnabled(false);
        conf.setOutputBufferSize(16000);
        conf.setPeriodicFlushFrequencyMilliSeconds(10);
        if(!isProducer){
            readbulknum = Integer.parseInt((String) p.remove(READBULKNUM));
            Preconditions.checkNotNull(readbulknum);
            try {
                namespace = DistributedLogNamespaceBuilder.newBuilder()
                        .conf(conf)
                        .uri(uri)
                        .build();
                DistributedLogManager dlm = namespace.openLog(streamName);
                DLSN lastDLSN;
                if(from == 0){
                    lastDLSN = DLSN.InitialDLSN;
                }else{
                    lastDLSN = dlm.getLastDLSN();
                }
                reader = FutureUtils.result(dlm.openAsyncLogReader(lastDLSN));
            }catch (IOException e){
                e.printStackTrace();
            }
        }else{
            serversetPaths = Arrays.asList(StringUtils.split((String) p.remove(SERVERSETPATHS), ','));
            finagleNames = Arrays.asList(StringUtils.split((String) p.remove(FINAGLENAMES), ','));
            hostConnectionLimit = Integer.parseInt((String) p.remove(HOSTCONNECTIONLIMIT));
            hostConnectionCoresize = Integer.parseInt((String) p.remove(HOSTCONNECTIONCORESIZE));
            thriftmux = Boolean.parseBoolean((String) p.remove(THRIFTMUX));
            iswriter = Boolean.parseBoolean((String)p.remove(ISWRITER));
            serverSets = createServerSets(serversetPaths);

            Preconditions.checkArgument(!finagleNames.isEmpty() || !serversetPaths.isEmpty(),
                    "either serverset paths or finagle-names required");
            Preconditions.checkArgument(hostConnectionCoresize > 0,
                    "host connection core size must be > 0");
            Preconditions.checkArgument(hostConnectionLimit > 0,
                    "host connection limit must be > 0");
            Preconditions.checkNotNull(iswriter);
            if(iswriter){
                try {
                    namespace = DistributedLogNamespaceBuilder.newBuilder()
                            .conf(conf)
                            .uri(uri)
                            .build();
                    DistributedLogManager dlm = namespace.openLog(streamName);
                    writer = FutureUtils.result(dlm.openAsyncLogWriter());
                    syncwriter = dlm.startLogSegmentNonPartitioned();
                }catch (IOException e){
                    e.printStackTrace();
                }
            }else{
                clientBuilder = ClientBuilder.get()
                        .hostConnectionLimit(hostConnectionLimit)
                        .hostConnectionCoresize(hostConnectionCoresize)
                        .tcpConnectTimeout(Duration$.MODULE$.fromMilliseconds(200))
                        .connectTimeout(Duration$.MODULE$.fromMilliseconds(200))
                        .requestTimeout(Duration$.MODULE$.fromSeconds(2));
                builder = DistributedLogClientBuilder.newBuilder()
                        .clientId(ClientId.apply("msbench-proxy-writer"))
                        .name("msbench-proxy-writer")
                        .thriftmux(thriftmux)
                        .clientBuilder(clientBuilder);
                if (serverSets.length == 0) {
                    String local = finagleNames.get(0);
                    String[] remotes = new String[finagleNames.size() - 1];
                    finagleNames.subList(1, finagleNames.size()).toArray(remotes);
                    builder = builder.finagleNameStrs(local, remotes);
                } else {
                    ServerSet local = serverSets[0].getServerSet();
                    ServerSet[] remotes = new ServerSet[serverSets.length - 1];
                    for (int i = 1; i < serverSets.length; i++) {
                        remotes[i-1] = serverSets[i].getServerSet();
                    }
                    builder = builder.serverSets(local, remotes);
                }
                client = builder.build();
            }

        }
    }

    protected DLZkServerSet[] createServerSets(List<String> serverSetPaths) {
        DLZkServerSet[] serverSets = new DLZkServerSet[serverSetPaths.size()];
        for (int i = 0; i < serverSets.length; i++) {
            String serverSetPath = serverSetPaths.get(i);
            serverSets[i] = DLZkServerSet.of(URI.create(serverSetPath), 60000);
        }
        return serverSets;
    }

    @Override
    public void initializeMS(ArrayList<String> streams) throws MSException {

    }

    @Override
    public void finalizeMS(ArrayList<String> streams) throws MSException {
        for (String stream : streams){
            try {
                if(client == null){
                    namespace.deleteLog(stream);
                }else{
                    client.delete(stream);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //final
    @Override
    public void send(boolean isSync, final byte[] msg, final WriteCallBack sentCallBack, final long requestTime) {
        if(isSync){
            //同步问题
            if(iswriter){
//                try {
//                    writer.write(new LogRecord(requestTime,msg)).toJavaFuture().get();
//                    sentCallBack.handleSentMessage(msg,requestTime);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                } catch (ExecutionException e) {
//                    e.printStackTrace();
//                }
                try {
                    syncwriter.write(new LogRecord(requestTime,msg));
                    sentCallBack.handleSentMessage(msg,requestTime);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("Sync-write failed!");
                }
            }else{
                try {
                    //todo use twitter's future's wait func
                    client.write(streamName,ByteBuffer.wrap(msg)).toJavaFuture().get();
                    // client.write(streamName,ByteBuffer.wrap(msg)).get();
                    sentCallBack.handleSentMessage(msg, requestTime);
                }catch (Exception e){
                    e.printStackTrace();
                    System.out.println("Sync-write failed!");
                }
            }

        }else{
            if(iswriter){
                writer.write(new LogRecord(requestTime,msg)).addEventListener(
                        new FutureEventListener<DLSN>() {
                            @Override
                            public void onFailure(Throwable cause) {
                                System.out.println("Async-write failed!");
                            }

                            @Override
                            public void onSuccess(DLSN value) {
                                sentCallBack.handleSentMessage(msg,requestTime);
                            }
                        }
                );
            }else{
                client.write(streamName, ByteBuffer.wrap(msg)).addEventListener(
                        new FutureEventListener<DLSN>() {
                            @Override
                            public void onSuccess(DLSN dlsn) {
                                sentCallBack.handleSentMessage(msg, requestTime);
                            }

                            @Override
                            public void onFailure(Throwable cause) {
                                //抛异常
                                if (cause instanceof DLException) {
                                    DLException dle = (DLException) cause;
                                    dle.printStackTrace();
                                    System.out.println("Async-write failed!");
                                }
                            }
                        }
                );
            }

        }
    }

    private boolean isRun = true;
    @Override
    public void read(final ReadCallBack readCallBack) {
        while (isRun) {
            reader.readNext().addEventListener(
                    new FutureEventListener<LogRecordWithDLSN>() {
                        @Override
                        public void onFailure(Throwable cause) {
                            if (cause instanceof DLException) {
                                DLException dle = (DLException) cause;
                                dle.printStackTrace();
                                System.out.println("read failed!");
                            }
                        }

                        @Override
                        public void onSuccess(LogRecordWithDLSN log) {
                            readCallBack.handleReceivedMessage(log.getPayload(), log.getTransactionId());
                        }
                    }
//        new FutureEventListener<List<LogRecordWithDLSN>>() {
//                    @Override
//                    public void onSuccess(List<LogRecordWithDLSN> logRecordWithDLSNs) {
//                        for (LogRecordWithDLSN log : logRecordWithDLSNs){
//                            readCallBack.handleReceivedMessage(log.getPayload(), log.getTransactionId());
//                        }
//                    }
//
//                    @Override
//                    public void onFailure(Throwable cause) {
//                        if (cause instanceof DLException) {
//                            DLException dle = (DLException) cause;
//                            dle.printStackTrace();
//                            System.out.println("read failed!");
//                        }
//                    }
//                }
            );
        }
    }

    @Override
    public void stopRead() {
        isRun = false;
    }

    @Override
    public void close() {
        if(reader!=null)
            reader.asyncClose();
        if(writer!=null)
            writer.asyncClose();
        if(syncwriter!=null)
            try {
                syncwriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        if(client!=null)
            client.close();
    }
}
