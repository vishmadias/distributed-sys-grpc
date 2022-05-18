package com.ds.grpc.client;

import com.ds.common.exception.InvalidSquareMatrixException;
import com.ds.common.util.MatrixUtil;
import com.ds.grpc.MatrixRequest;
import com.ds.grpc.MatrixResponse;
import com.ds.grpc.MatrixServiceGrpc;
import com.ds.grpc.MatrixServiceGrpc.MatrixServiceBlockingStub;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static com.ds.common.util.MatrixUtil.convertToSquareMatrix;

@Service
public class GRPCClientService {

    private int[] stubPorts = {8081, 8082, 8083, 8084, 8085, 8086, 8087, 8088};

    @Value("${com.ds.grpc.client.serverIp}")
    private String serverIp;

    private ManagedChannel[] channelList;
    private MatrixServiceBlockingStub[] stubList;
    private BlockingQueue<Integer> stubIndicesQueue = new LinkedBlockingQueue<>(stubPorts.length);

    @PostConstruct
    public void init() throws InterruptedException {
        channelList = createChannels();
        stubList = createStubs();
    }

    @PreDestroy
    public void destroy() {
        for (ManagedChannel channel : channelList) {
            channel.shutdown();
        }
    }

    /***
     *  Multiplies given 2 matrices with deadline based scaling
     * @param matrixString1
     * @param matrixString2
     * @param deadline : deadline in nanoseconds
     * @return
     * @throws InvalidSquareMatrixException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public String multiplyMatrices(String matrixString1, String matrixString2, long deadline) throws InvalidSquareMatrixException, ExecutionException, InterruptedException {

        System.out.println(" ================= Executing multiply in client =============== ");

        int[][] A = convertToSquareMatrix(matrixString1);
        int[][] B = convertToSquareMatrix(matrixString2);

        System.out.println("Matrix 1: " + MatrixUtil.encodeMatrix(A));
        System.out.println("Matrix 2: " + MatrixUtil.encodeMatrix(B));

        int[][] multipliedMatrixBlock = multiplyMatrixBlock(A, B, deadline);
        return MatrixUtil.encodeMatrix(multipliedMatrixBlock);
    }


    /**
     * Taking the stub indices from the queue in order and add the same to back of the queue to ensure fairness.
     *
     * @param num
     * @return indices of the num stubs
     * @throws InterruptedException
     */
    private int[] takeFromStubIndicesQueue(int num) throws InterruptedException {
        int[] indices = new int[num];
        for (int i = 0; i < num; i++) {
            indices[i] = this.stubIndicesQueue.take();
            this.stubIndicesQueue.add(indices[i]);
        }
        return indices;
    }


    private MatrixServiceBlockingStub[] createStubs() {
        MatrixServiceBlockingStub[] stubs = new MatrixServiceBlockingStub[stubPorts.length];

        for (int i = 0; i < channelList.length; i++) {
            stubs[i] = MatrixServiceGrpc.newBlockingStub(channelList[i]);
        }

        for (int i = 0; i < stubPorts.length; i++) {
            stubIndicesQueue.add(i);
        }

        return stubs;
    }

    private ManagedChannel[] createChannels() {
        ManagedChannel[] chans = new ManagedChannel[stubPorts.length];
        System.out.println("Connecting to server at: " + serverIp);

        for (int i = 0; i < stubPorts.length; i++) {
            chans[i] = ManagedChannelBuilder.forAddress(serverIp, stubPorts[i])
                    .keepAliveWithoutCalls(true)
                    .usePlaintext()
                    .build();
        }
        return chans;
    }


    /**
     * Add integer matrices via gRPC
     */

    private int[][] addBlock(int A[][], int B[][], int stubIndex) {
        System.out.println("Calling addBlock on server " + (stubIndex + 1));
        MatrixRequest request = generateRequest(A, B);
        MatrixResponse matrixAddResponse = this.stubList[stubIndex].addBlock(request);
        int[][] summedMatrix = MatrixUtil.decodeMatrix(matrixAddResponse.getMatrix());
        return summedMatrix;
    }


    /**
     * Multiply integer matrices via gRPC
     */
    private int[][] multiplyBlock(int A[][], int B[][], int stubIndex) {
        System.out.println("Calling multiplyBlock on server " + (stubIndex + 1));
        MatrixRequest request = generateRequest(A, B);
        MatrixResponse matrixMultiplyResponse = this.stubList[stubIndex].multiplyBlock(request);
        int[][] multipliedMatrix = MatrixUtil.decodeMatrix(matrixMultiplyResponse.getMatrix());
        return multipliedMatrix;
    }


    /**
     * encode the matrices and return a MatrixRequest object
     */

    private static MatrixRequest generateRequest(int A[][], int B[][]) {
        String matrixA = MatrixUtil.encodeMatrix(A);
        String matrixB = MatrixUtil.encodeMatrix(B);

        return MatrixRequest.newBuilder()
                .setMatrixA(matrixA)
                .setMatrixB(matrixB)
                .build();
    }


    /***
     *  Multiply given 2 matrices by multiplying & adding blocks
     * @param A
     * @param B
     * @param deadline
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private int[][] multiplyMatrixBlock(int[][] A, int[][] B, long deadline) throws InterruptedException, ExecutionException {

        // split matrices into 8 smaller blocks
        HashMap<String, int[][]> blocks = splitBlocks(A, B);

        // get first gRPC server stub
        int firstStubIndex = takeFromStubIndicesQueue(1)[0];

        // footprint algorithm
        long startTime = System.nanoTime();

        // async calls to the multiplyBlock function
        CompletableFuture<int[][]> A1A2Future = CompletableFuture.supplyAsync(() -> multiplyBlock(blocks.get("A1"), blocks.get("A2"), firstStubIndex));

        // Once async function completes execution, this will be called
        int[][] A1A2 = A1A2Future.get();
        long endTime = System.nanoTime();
        long footprint = endTime - startTime;

        System.out.println("Footprint for current : " + footprint);

        // processing remaining blocks

        long numBlockCalls = 11L;
        int serverCount = (int) Math.ceil((float) footprint * (float) numBlockCalls / (float) deadline);

        System.out.println("Original number of server requirement : " + serverCount);

        serverCount = Math.min(serverCount, 8);

        System.out.println("Using " + serverCount + " servers for the rest of calculation");
        System.out.println("==============================");

        // takes stub indices from queue (FIFO) -> takes least recently used stub first
        int[] indices = takeFromStubIndicesQueue(serverCount);

        // adding list of stub indices to a thread safe queue
        BlockingQueue<Integer> indexQueue = new LinkedBlockingQueue<>((int) numBlockCalls);

        int i = 0;
        while (indexQueue.size() != numBlockCalls) {
            if (indices.length == i) {
                i = 0;
            }
            indexQueue.add(indices[i]);
            i++;
        }

        // async calls to the gRPC server
        // 1 for each number of call to be made for entire block process ( add & multiply) -> 11

        // multiplications
        CompletableFuture<int[][]> B1C2 = CompletableFuture.supplyAsync(() -> {
            try {
                return multiplyBlock(blocks.get("B1"), blocks.get("C2"), indexQueue.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        });

        CompletableFuture<int[][]> A1B2 = CompletableFuture.supplyAsync(() -> {
            try {
                return multiplyBlock(blocks.get("A1"), blocks.get("B2"), indexQueue.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        });

        CompletableFuture<int[][]> B1D2 = CompletableFuture.supplyAsync(() -> {
            try {
                return multiplyBlock(blocks.get("B1"), blocks.get("D2"), indexQueue.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        });

        CompletableFuture<int[][]> C1A2 = CompletableFuture.supplyAsync(() -> {
            try {
                return multiplyBlock(blocks.get("C1"), blocks.get("A2"), indexQueue.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        });

        CompletableFuture<int[][]> D1C2 = CompletableFuture.supplyAsync(() -> {
            try {
                return multiplyBlock(blocks.get("D1"), blocks.get("C2"), indexQueue.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        });

        CompletableFuture<int[][]> C1B2 = CompletableFuture.supplyAsync(() -> {
            try {
                return multiplyBlock(blocks.get("C1"), blocks.get("B1"), indexQueue.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        });

        CompletableFuture<int[][]> D1D2 = CompletableFuture.supplyAsync(() -> {
            try {
                return multiplyBlock(blocks.get("D1"), blocks.get("D2"), indexQueue.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        });

        // additions
        CompletableFuture<int[][]> A3 = CompletableFuture.supplyAsync(() -> {
            try {
                return addBlock(A1A2, B1C2.get(), indexQueue.take());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            return null;
        });

        CompletableFuture<int[][]> B3 = CompletableFuture.supplyAsync(() -> {
            try {
                return addBlock(A1B2.get(), B1D2.get(), indexQueue.take());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            return null;
        });

        CompletableFuture<int[][]> C3 = CompletableFuture.supplyAsync(() -> {
            try {
                return addBlock(C1A2.get(), D1C2.get(), indexQueue.take());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            return null;
        });

        CompletableFuture<int[][]> D3 = CompletableFuture.supplyAsync(() -> {
            try {
                return addBlock(C1B2.get(), D1D2.get(), indexQueue.take());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            return null;
        });

        // join the remote calculations back together
        int[][] res = joinBlocks(A3.get(), B3.get(), C3.get(), D3.get());

        System.out.println("================ Calculations Complete ===============");
        return res;
    }

	/***
	 *  Join the blocks to retain original resulting matrix
	 * @param A3
	 * @param B3
	 * @param C3
	 * @param D3
	 * @return
	 */
    private int[][] joinBlocks(int[][] A3, int[][] B3, int[][] C3, int[][] D3) {
        int n = A3.length;
        int bSize = n / 2;
        int[][] res = new int[n][n];

        for (int i = 0; i < bSize; i++) {
            for (int j = 0; j < bSize; j++) {
                res[i][j] = A3[i][j];
            }
        }
        for (int i = 0; i < bSize; i++) {
            for (int j = bSize; j < n; j++) {
                res[i][j] = B3[i][j - bSize];
            }
        }
        for (int i = bSize; i < n; i++) {
            for (int j = 0; j < bSize; j++) {
                res[i][j] = C3[i - bSize][j];
            }
        }
        for (int i = bSize; i < n; i++) {
            for (int j = bSize; j < n; j++) {
                res[i][j] = D3[i - bSize][j - bSize];
            }
        }
        return res;
    }

    /***
     *  Splits given 2 matrices into 8 blocks (for divide & conquer approach)
     * @param A
     * @param B
     * @return
     */
    private HashMap<String, int[][]> splitBlocks(int[][] A, int[][] B) {

        int n = A.length;
        int bSize = n / 2;

        int[][] A1 = new int[n][n];
        int[][] A2 = new int[n][n];
        int[][] B1 = new int[n][n];
        int[][] B2 = new int[n][n];
        int[][] C1 = new int[n][n];
        int[][] C2 = new int[n][n];
        int[][] D1 = new int[n][n];
        int[][] D2 = new int[n][n];

        for (int i = 0; i < bSize; i++) {
            for (int j = 0; j < bSize; j++) {
                A1[i][j] = A[i][j];
                A2[i][j] = B[i][j];
            }
        }
        for (int i = 0; i < bSize; i++) {
            for (int j = bSize; j < n; j++) {
                B1[i][j - bSize] = A[i][j];
                B2[i][j - bSize] = B[i][j];
            }
        }
        for (int i = bSize; i < n; i++) {
            for (int j = 0; j < bSize; j++) {
                C1[i - bSize][j] = A[i][j];
                C2[i - bSize][j] = B[i][j];
            }
        }
        for (int i = bSize; i < n; i++) {
            for (int j = bSize; j < n; j++) {
                D1[i - bSize][j - bSize] = A[i][j];
                D2[i - bSize][j - bSize] = B[i][j];
            }
        }

        HashMap<String, int[][]> blocks = new HashMap<>();
        blocks.put("A1", A1);
        blocks.put("A2", A2);
        blocks.put("B1", B1);
        blocks.put("B2", B2);
        blocks.put("C1", C1);
        blocks.put("C2", C2);
        blocks.put("D1", D1);
        blocks.put("D2", D2);

        return blocks;
    }
}
