package hdfs;

import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.tuple.Tuple;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 计数以改变Hdfs写入文件的位置，当写入10次的时候，则更改写入文件，更改名字取决于 “TimesFileNameFormat”
 * 这个类是线程安全
 */

public class CountStrRotationPolicy implements FileRotationPolicy {


    private SimpleDateFormat df = new SimpleDateFormat("yyyyMM");

    private String date =  null;

    private int count = 0;

    public CountStrRotationPolicy(){
        this.date =  df.format(new Date());
//        this.date = df.format(new Date());
    }


    /**
     * Called for every tuple the HdfsBolt executes.
     *
     * @param tuple  The tuple executed.
     * @param offset current offset of file being written
     * @return true if a file rotation should be performed
     */
    @Override
    public boolean mark(Tuple tuple, long offset) {
        count ++;
        if(count == 10) {
            System.out.print("num :" +count + "   ");
            count = 0;
            return true;

        }
        else {
            return false;
        }
    }

    /**
     * Called after the HdfsBolt rotates a file.
     */
    @Override
    public void reset() {

    }

    @Override
    public FileRotationPolicy copy() {
        return new CountStrRotationPolicy();
    }


}
