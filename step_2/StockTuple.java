import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
public class StockTuple implements Writable{

    private Integer ma50;
    private Integer ma100;
    private Integer ma200;
    private Integer count;

    public MinMaxCountTuple(){
	this.min=0;
	this.max=0;
	this.count=0;
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
    // Not used
    }

    public void write(DataOutput out) throws IOException {
    // Not used
    }

    public String toString() {
	return ma50 + "\t" + ma100 + "\t" + ma200 + "\t" + count;
    }
}
