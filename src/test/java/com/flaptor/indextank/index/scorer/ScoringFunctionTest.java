package com.flaptor.indextank.index.scorer;

import com.flaptor.indextank.index.scorer.parser.ScoreFormulaParser;
import com.flaptor.indextank.query.QueryVariables;
import com.flaptor.indextank.query.QueryVariablesImpl;

public class ScoringFunctionTest {


    public static void main(String[] args) throws Exception {
        Boosts doc_vars = new Boosts() {
            public float getBoost(int idx) {return idx/10f;}
            public int getBoostCount() {return 1;}
            public int getTimestamp() {return 0;}
        };
        QueryVariables query_vars = new QueryVariablesImpl(new Double[] {0.0d,1.0d,2.0d,3.0d,4.0d,5.0d,6.0d,7.0d,8.0d,9.0d}, null, null);
        double score = 0.5d;
        int age = 100;

        String input = "log(age) + 5";
        ScoreFunction func = ScoreFormulaParser.parseFormula(1, input);
        double result = func.score(score, age, doc_vars, query_vars);
        System.out.println("result: "+result);

        ScoreFunction f2 = ScoreFormulaParser.parseFormula(2, "miles(d[0], d[1], q[0], q[1])");
        Boosts doc_vars2 = new Boosts() {
            public float getBoost(int idx) {
                float[] boosts = { 30.5f, -97.3f };
                return boosts[idx];
            }
            public int getBoostCount() {return 1;}
            public int getTimestamp() {return 0;}
        };
        QueryVariables query_vars2 =
                new QueryVariablesImpl(new Double[] {30.0, -97.0}, null, null);
        result = f2.score(score, age, doc_vars2, query_vars2);
        System.out.println("Result2: " + result);

        QueryVariables query_vars3 = new QueryVariablesImpl(new Double[] { }, 30.0f, -97.0f);

        ScoreFunction f3 = ScoreFormulaParser.parseFormula(2, "miles(d[0], d[1], ipgeo.lat, ipgeo.lon)");
        //ScoreFunction f3 = ScoreFormulaParser.parseFormula(2, "ipgeo.lat + ipgeo.lon");
        System.out.println("f3: " + f3);
        result = f3.score(0.625, 3600, doc_vars2, query_vars3);
        System.out.println("f3 result: " + result);
    }
}
