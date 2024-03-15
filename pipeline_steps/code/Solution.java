public class Solution {

    public static void main(String[] args) {
        // Example Sudoku puzzle
        int[][] sudoku = {
                { 5, 3, 0, 0, 7, 0, 0, 0, 0 },
                { 6, 0, 0, 1, 9, 5, 0, 0, 0 },
                { 0, 9, 8, 0, 0, 0, 0, 6, 0 },
                { 8, 0, 0, 0, 6, 0, 0, 0, 3 },
                { 4, 0, 0, 8, 0, 3, 0, 0, 1 },
                { 7, 0, 0, 0, 2, 0, 0, 0, 6 },
                { 0, 6, 0, 0, 0, 0, 2, 8, 0 },
                { 0, 0, 0, 4, 1, 9, 0, 0, 5 },
                { 0, 0, 0, 0, 8, 0, 0, 7, 9 }
        };

        // Solve the Sudoku puzzle
        int[][] solution = solveSudoku(sudoku);

        // Print the solved Sudoku
        printSudoku(solution);
    }

    // Solve the Sudoku puzzle
    public static int[][] solveSudoku(int[][] sudoku) {
        solve(sudoku);
        return sudoku;
    }

    // Backtracking algorithm to solve the Sudoku puzzle
    private static boolean solve(int[][] sudoku) {
        for (int row = 0; row < 9; row++) {
            for (int col = 0; col < 9; col++) {
                if (sudoku[row][col] == 0) {
                    // Try each number from 1 to 9
                    for (int num = 1; num <= 9; num++) {
                        if (isValidMove(sudoku, row, col, num)) {
                            // Place the number and recursively solve the rest of the puzzle
                            sudoku[row][col] = num;

                            if (solve(sudoku)) {
                                return true;
                            }

                            // If placing the number doesn't lead to a solution, backtrack
                            sudoku[row][col] = 0;
                        }
                    }
                    return false; // No valid number for this cell, backtrack
                }
            }
        }
        return true; // The puzzle is solved
    }

    // Check if placing a number in a cell is a valid move
    private static boolean isValidMove(int[][] sudoku, int row, int col, int num) {
        return !usedInRow(sudoku, row, num) &&
                !usedInCol(sudoku, col, num) &&
                !usedInBox(sudoku, row - row % 3, col - col % 3, num);
    }

    // Check if a number is used in the same row
    private static boolean usedInRow(int[][] sudoku, int row, int num) {
        for (int col = 0; col < 9; col++) {
            if (sudoku[row][col] == num) {
                return true;
            }
        }
        return false;
    }

    // Check if a number is used in the same column
    private static boolean usedInCol(int[][] sudoku, int col, int num) {
        for (int row = 0; row < 9; row++) {
            if (sudoku[row][col] == num) {
                return true;
            }
        }
        return false;
    }

    // Check if a number is used in the same 3x3 box
    private static boolean usedInBox(int[][] sudoku, int boxStartRow, int boxStartCol, int num) {
        for (int row = 0; row < 3; row++) {
            for (int col = 0; col < 3; col++) {
                if (sudoku[row + boxStartRow][col + boxStartCol] == num) {
                    return true;
                }
            }
        }
        return false;
    }

    // Print the Sudoku grid
    private static void printSudoku(int[][] sudoku) {
        for (int row = 0; row < 9; row++) {
            for (int col = 0; col < 9; col++) {
                System.out.print(sudoku[row][col] + " ");
            }
            System.out.println();
        }
    }
}