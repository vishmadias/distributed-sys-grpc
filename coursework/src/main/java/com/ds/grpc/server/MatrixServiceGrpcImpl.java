package com.ds.grpc.server;

import com.ds.common.util.MatrixUtil;
import com.ds.grpc.MatrixRequest;
import com.ds.grpc.MatrixResponse;
import com.ds.grpc.MatrixServiceGrpc;
import io.grpc.stub.StreamObserver;

import javax.el.MethodNotFoundException;

public class MatrixServiceGrpcImpl extends MatrixServiceGrpc.MatrixServiceImplBase {

	private int threadNumber;

	MatrixServiceGrpcImpl(int threadNumber) {
		this.threadNumber = threadNumber;
	}

	@Override
	public void addBlock(MatrixRequest request, StreamObserver<MatrixResponse> responseObserver) {
		System.out.println("addBlock called on server "+ threadNumber);
		System.out.println("*****************************");
		requestHandler(request, responseObserver, OperationType.ADD);
	}

	@Override
	public void multiplyBlock(MatrixRequest request, StreamObserver<MatrixResponse> responseObserver) {
		System.out.println("multiplyBlock called on server " + threadNumber);
		System.out.println("*****************************");
		requestHandler(request, responseObserver, OperationType.MULTIPLY);
	}

	/**
	 * Handles the gRPC request for both addBlock and multiplyBlock methods
	 */
	private void requestHandler(MatrixRequest request, StreamObserver<MatrixResponse> responseObserver, OperationType operation) throws MethodNotFoundException {

		// decode matrixA and matrixB from the request
		int[][] decodedMatrixA = MatrixUtil.decodeMatrix(request.getMatrixA());
		int[][] decodedMatrixB = MatrixUtil.decodeMatrix(request.getMatrixB());

		int[][] result;

		switch(operation) {
			case ADD:
				result = addMatrices(decodedMatrixA, decodedMatrixB);
				break;
			case MULTIPLY:
				result = multiplyMatrices(decodedMatrixA, decodedMatrixB);
				break;
			default:
				System.out.println("Unidentified Operation: " + operation);
				throw new MethodNotFoundException("Couldn't find method: " + operation);
		}

		// encode the resultant matrix as a string
		String encodedMatrix = MatrixUtil.encodeMatrix(result);

		// generate the matrix response object
		MatrixResponse response = MatrixResponse.newBuilder()
			.setMatrix(encodedMatrix)
			.build();

		// send response of gRPC
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}


	private static int[][] addMatrices(int[][] matrixA, int[][]matrixB) {

		int n = matrixA.length;
		int[][] result = new int[n][n];

		for (int i=0; i<result.length; i++) {
			for (int j=0; j < result.length; j++) {
				result[i][j] = matrixA[i][j] + matrixB[i][j];
			}
		}
		return result; 
	}


	private static int[][] multiplyMatrices(int A[][], int B[][]) {

		int n = A.length;
		int blockSize = n/2;
		int C[][]= new int[n][n];

		/* for size 4
		C[0][0]=A[0][0]*B[0][0]+A[0][1]*B[1][0];
		C[0][1]=A[0][0]*B[0][1]+A[0][1]*B[1][1];
		C[1][0]=A[1][0]*B[0][0]+A[1][1]*B[1][0];
		C[1][1]=A[1][0]*B[0][1]+A[1][1]*B[1][1];
		*/

        for(int i=0;i<blockSize;i++){
            for(int j=0;j<blockSize;j++){
                for(int k=0;k<blockSize;k++){
                    C[i][j]+=(A[i][k]*B[k][j]);
                }
            }
        }

        return C;
	}
}
