import com.google.common.io.Files;
import exercise_2.Exercise_2;
import exercise_3.Exercise_3;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import exercise_1.Exercise_1;
import utils.Utils;

public class Main {

	static String HADOOP_COMMON_PATH = "C:\\Users\\Arnald\\Desktop\\ARNALD\\KNOWLEDGE\\PROJECTE MASTER\\MASTER\\Hands-On Experience\\Spark Graphs\\spark-graphs-lab02\\src\\main\\resources\\winutils";
	
	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);

		SparkConf conf = new SparkConf().setAppName("SparkGraphs_II").setMaster("local[*]");
		JavaSparkContext ctx = new JavaSparkContext(conf);
		ctx.setCheckpointDir(Files.createTempDir().getAbsolutePath());
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(
                Level.ERROR);

		if (args.length != 1) throw new Exception("Parameter expected: exercise number");

		if (args[0].equals("exercise1")) {
		    Exercise_1.maxValue(ctx);
        }
        else if (args[0].equals("exercise2")) {
            Exercise_2.shortestPaths(ctx);
        }
        else if (args[0].equals("exercise3")) {
            Exercise_3.shortestPathsExt(ctx);
        }
        else {
		    throw new Exception("Wrong exercise number");
        }

	}

}
