import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
public class StockTuple implements Writable{

    public Integer ma50;
    public Integer ma100;
    public Integer ma200;
    public Integer count;

    public StockTuple(){
	this.ma50=0;
	this.ma100=0;
	this.ma200=0;
	this.count=1;
    }

    public Integer getMa50(){
	return ma50;
    }
    public void setMa50(Integer ma50){
	this.ma50=ma50;
    }

    public Integer getMa100(){
	return ma100;
    }
    public void setMa100(Integer ma100){
	this.ma100=ma100;
    }
    public Integer getMa200(){
	return ma200;
    }
    public void setMa200(Integer ma200){
	this.ma200=ma200;
    }
    public Integer getCount(){
	return count;
    }
    public void setCount(Integer count){
	this.count=count;
    }
    public void readFields(DataInput in) throws IOException {
	ma50 = in.readInt();
	ma100 = in.readInt();
	ma200 = in.readInt();
	count = in.readInt();

	// Not used
    }

    public void write(DataOutput out) throws IOException {
	out.writeInt(ma50);
	out.writeInt(ma100);
	out.writeInt(ma200);
	out.writeInt(count);
	// Not used
    }

    public String toString() {
	return ma50 + "\t" + ma100 + "\t" + ma200 + "\t" + count;
    }
}
