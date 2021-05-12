import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import common._

object Salting {

	val spark: SparkSession = SparkSession.builder()
		.appName("Fixing Data Skews")
		.master("local[*]")
		.getOrCreate()

	// deactivate broadcast joins
	spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

	import spark.implicits._

	val guitars: Dataset[Guitar] = Seq.fill(40000)(DataGenerator.randomGuitar()).toDS
	val guitarSales: Dataset[GuitarSale] = Seq.fill(200000)(DataGenerator.randomGuitarSale()).toDS

	/**
	 * A Guitar is similar to a GuitarSale if<br />
	 * .  same make and model<br />
	 * .  abs(guitar.soundScore - guitarSale.soundScore) <= 0.1<br />
	 *
	 * [problem]:<br />
	 * .  for every Guitar, avg(sale prices of ALL SIMILAR GuitarSales)<br />
	 * .  Gibson L-00, config "sadfhja", sound 4.3 <br />
	 *
	 * [task]:<br />
	 * .  compute avg-sale prices of ALL GuitarSales of (Gibson L-00 with sound quality between 4.2 and 4.4)<br />
	 */

	def noSkewSolution(): Unit = {
		/**
		 * [salting][explode(lit((0 to 99).toArray))]<br />
		 * . interval 0-99<br />
		 * . for every [row.a] => [row.a]x(100) | multiplying the guitars DS x100<br />
		 *
		 * [monotonically_increasing_id][uniform distribution]<br />
		 * A column expression that generates monotonically increasing 64-bit integers.<br />
		 * The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.<br />
		 * The current implementation puts the partition ID in the upper 31 bits,<br />
		 * and the record number within each partition in the lower 33 bits.<br />
		 * The assumption is that the data frame has less than 1 billion partitions, and each partition has less than 8 billion records.<br />
		 *
		 * As an example, consider a DataFrame with two partitions, each with 3 records. This expression would return the following IDs:<br />
		 * 0, 1, 2, 8589934592 (1L << 33), 8589934593, 8589934594.<br />
		 *
		 */
		val explodedGuitars = guitars.withColumn("salt", explode(lit((0 to 99).toArray)))
		val saltedGuitarSales = guitarSales.withColumn("salt", monotonically_increasing_id() % 100)

		val nonSkewedJoin = explodedGuitars.join(saltedGuitarSales, Seq("make", "model", "salt")) // uniform data-distribution
			.where(abs(saltedGuitarSales("soundScore") - explodedGuitars("soundScore")) <= 0.1)
			.groupBy("configurationId")
			.agg(avg("salePrice").as("averagePrice"))

		nonSkewedJoin.explain()
		nonSkewedJoin.show()
	}

	def main(args: Array[String]): Unit = {
		noSkewSolution()
		Thread.sleep(1000000)
	}
}