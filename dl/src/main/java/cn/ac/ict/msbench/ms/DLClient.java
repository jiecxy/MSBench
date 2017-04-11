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

    DistributedLogConfiguration conf = null;
    AsyncLogReader reader = null;
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

    public DLClient(String streamName, boolean isProducer, Properties p,int from) {
        super(streamName, isProducer, p,from);
        if(!isProducer){
            uri = URI.create((String) p.remove(DLURI));
            readbulknum = Integer.parseInt((String) p.remove(READBULKNUM));
            Preconditions.checkNotNull(uri);
            Preconditions.checkNotNull(readbulknum);
            conf = new DistributedLogConfiguration();
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
            serverSets = createServerSets(serversetPaths);

            Preconditions.checkArgument(!finagleNames.isEmpty() || !serversetPaths.isEmpty(),
                    "either serverset paths or finagle-names required");
            Preconditions.checkArgument(hostConnectionCoresize > 0,
                    "host connection core size must be > 0");
            Preconditions.checkArgument(hostConnectionLimit > 0,
                    "host connection limit must be > 0");
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
    public void send(boolean isSync, final byte[] msg, final WriteCallBack sentCallBack, final long requestTimeInNano) {
        if(isSync){
            //同步问题
            try {
                //todo use twitter's future's wait func
                client.write(streamName,ByteBuffer.wrap(msg)).toJavaFuture().get();
               // client.write(streamName,ByteBuffer.wrap(msg)).get();
                sentCallBack.handleSentMessage(msg, requestTimeInNano);
            }catch (Exception e){
                e.printStackTrace();
                System.out.println("sync-write failed!");
            }
        }else{
            client.write(streamName, ByteBuffer.wrap(msg)).addEventListener(
                    new FutureEventListener<DLSN>() {
                        @Override
                        public void onSuccess(DLSN dlsn) {
                            sentCallBack.handleSentMessage(msg, requestTimeInNano);
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

    @Override
    public void read(final ReadCallBack readCallBack, final long requestTimeInNano) {
        reader.readBulk(readbulknum).addEventListener(
                new FutureEventListener<List<LogRecordWithDLSN>>() {
                    @Override
                    public void onSuccess(List<LogRecordWithDLSN> logRecordWithDLSNs) {
                        for (LogRecordWithDLSN log : logRecordWithDLSNs){
                            readCallBack.handleReceivedMessage(log.getPayload(), requestTimeInNano, log.getTransactionId()*1000000);
                        }
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        if (cause instanceof DLException) {
                            DLException dle = (DLException) cause;
                            dle.printStackTrace();
                            System.out.println("read failed!");
                        }
                    }
                }
        );
    }

    @Override
    public void close() {
        if(reader!=null)
            reader.asyncClose();
        if(client!=null)
            client.close();
    }
}
