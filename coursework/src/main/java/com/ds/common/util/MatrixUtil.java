package com.ds.common.util;

import com.ds.common.exception.InvalidSquareMatrixException;

import java.util.Arrays;

import static com.google.common.math.IntMath.isPowerOfTwo;

public class MatrixUtil {

    /***
     *  Convert matrix string to array (2D)
     * @param matrixString
     * @return
     */
    public static int[][] decodeMatrix(String matrixString) {

        int row = 0;
        int col = 0;
        for (int i = 0; i < matrixString.length(); i++) {
            if (matrixString.charAt(i) == '[') {
                row++;
            }
        }
        row--;
        for (int i = 0; ; i++) {
            if (matrixString.charAt(i) == ',') {
                col++;
            }
            if (matrixString.charAt(i) == ']') {
                break;
            }
        }
        col++;

        int[][] out = new int[row][col];

        matrixString = matrixString.replaceAll("\\[", "").replaceAll("\\]", "");

        String[] s1 = matrixString.split(", ");

        int j = -1;
        for (int i = 0; i < s1.length; i++) {
            if (i % col == 0) {
                j++;
            }
            out[j][i % col] = Integer.parseInt(s1[i]);
        }
        return out;
    }

    /***
     *  Convert 2D array of a matrix to string
     * @param matrix
     * @return
     */
    public static String encodeMatrix(int[][] matrix) {
        return Arrays.deepToString(matrix);
    }

    /***
     *  Converts given matrix to square matrix
     * @param matrixString
     * @return
     * @throws InvalidSquareMatrixException : raises if the input string is not square matrix
     */
    public static int[][] convertToSquareMatrix(String matrixString) throws InvalidSquareMatrixException {
        // convert matrix string to lines and columns
        String[] lines = matrixString.trim().split("\n");
        String[] columns = lines[0].trim().split(" ");

        // init the matrix array
        int[][] matrixArray = new int[lines.length][columns.length];

        if (lines.length < 1 || columns.length < 1) {
            throw new InvalidSquareMatrixException("Invalid matrix:\n\n" + matrixString + "\n\nmatrix must have rows and columns",
                    new Error("matrix must have rows and columns"));
        }

        if (lines.length != columns.length) {
            throw new InvalidSquareMatrixException("Invalid matrix:\n\n" + matrixString + "\n\nmatrix must same number of rows and columns",
                    new Error("matrix must same number of rows and columns"));
        }

        if (!isPowerOfTwo(lines.length) || !isPowerOfTwo(columns.length)) {
            throw new InvalidSquareMatrixException("Invalid matrix:\n\n" + matrixString + "\n\nmatrix row and column size must be a power of 2",
                    new Error("matrix row and column size must be a power of 2"));
        }

        try {
            // loop through each matrix value and assign to matrixArray
            for (int i = 0; i < lines.length; i++) {
                String[] matrixValues = lines[i].trim().split(" ");
                if (matrixValues.length != columns.length) {
                    throw new InvalidSquareMatrixException("Invalid matrix:\n\n" + matrixString + "\n\nmatrix row length not equal",
                            new Error("matrix row length not equal"));
                }
                for (int j = 0; j < matrixValues.length; j++) {
                    matrixArray[i][j] = Integer.parseInt(matrixValues[j]);
                }
            }
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            throw new InvalidSquareMatrixException("Invalid Square Matrix:\n\n" + matrixString, e);
        }

        return matrixArray;
    }

}
