package kz.sdu.bdt.pig;

import kz.sdu.bdt.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.StringTokenizer;

public class Tokenizer extends EvalFunc<DataBag> {

    TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();

    @Override
    public DataBag exec(Tuple input) throws IOException {
        try {
            DataBag output = mBagFactory.newDefaultBag();
            Object o = input.get(0);
            if (!(o instanceof String)) {
                throw new IOException("Expected input to be chararray, but  got " + o.getClass().getName());
            }
            StringTokenizer tok = new StringTokenizer(StringUtils.clearString(o.toString()));
            while (tok.hasMoreTokens())  {
                String token = tok.nextToken();
                if (!StringUtils.STOP_WORDS.contains(token) && token.length() > 3)
                    output.add(mTupleFactory.newTuple(token));
            }
            return output;
        } catch (ExecException ee) {
            // error handling goes here
        }
        return null;
    }
}
