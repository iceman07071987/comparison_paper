package kz.sdu.bdt.pig;

import kz.sdu.bdt.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class MinHasher extends EvalFunc<String> {
    @Override
    public String exec(Tuple tuple) throws IOException {
        Object o = tuple.get(0);
        if (!(o instanceof String)) {
            throw new IOException("Expected input to be chararray, but  got " + o.getClass().getName());
        }
        return StringUtils.createSongHashes(o.toString());
    }
}
