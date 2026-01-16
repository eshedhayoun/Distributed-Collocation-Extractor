package com.example.llr;

public class LLRUtils {
    
    private static double logL(long k, long n, double x) {
        if (x <= 0.0 || x >= 1.0) return 0.0;
        if (k == 0) return (n - k) * Math.log(1.0 - x);
        if (k == n) return k * Math.log(x);
        return k * Math.log(x) + (n - k) * Math.log(1.0 - x);
    }
    
    public static double calculateLLR(long c1, long c2, long c12, long N) {
        if (c1 <= 0 || c2 <= 0 || c12 <= 0 || N <= 0) return 0.0;
        if (c12 > c1 || c12 > c2) return 0.0;
        
        double p = (double) c2 / N;
        double p1 = (double) c12 / c1;
        double p2 = (double) (c2 - c12) / (N - c1);
        
        long k1 = c12;
        long n1 = c1;
        long k2 = c2 - c12;
        long n2 = N - c1;
        
        double logLambda = logL(k1, n1, p) 
                         + logL(k2, n2, p) 
                         - logL(k1, n1, p1) 
                         - logL(k2, n2, p2);
        
        return -2.0 * logLambda;
    }
}
