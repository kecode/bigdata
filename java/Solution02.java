public class Solution02 {
    public static String insertDigit(String score, String digit){
        for (int i = 0; i < digit.length(); i++) {
            int d = Integer.parseInt(digit.substring(i,i+1));
            for(int j =0; j < score.length(); j ++) {
                int s = Integer.parseInt(score.substring(j,j+1));
                if (d > s) {
                    if (j == 0) {
                        return digit + score;
                    } else if (0 < j && j < score.length() -1){
                        return score.substring(0, j) + digit + score.substring(j, score.length());
                    } else {
                        return score + digit;
                    }
                }

            }
        }
        return  score + digit;
    }

    public static void main(String[] args) {
        String s = insertDigit("298100", "293");
        System.out.println(s);
    }
}
