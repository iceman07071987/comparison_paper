package kz.sdu.bdt.pig;

import kz.sdu.bdt.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class JaccardSimilarity extends EvalFunc<Double> {
    @Override
    public Double exec(Tuple tuple) throws IOException {
        Object o = tuple.get(0);
        Object o2 = tuple.get(1);
        if (!(o instanceof String) || !(o2 instanceof String)) {
            throw new IOException("Expected input to be chararray, but  got " + o.getClass().getName());
        }

        String[] h1 = o.toString().split(" ");
        String[] h2 = o2.toString().split(" ");

        return StringUtils.jaccardSimilarity(h1, h2);
    }
}
