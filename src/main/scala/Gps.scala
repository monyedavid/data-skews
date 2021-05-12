import common.{DataGenerator, Laptop, LaptopOffer}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Gps {

	val spark: SparkSession = SparkSession.builder()
		.appName("RDD Skewed Joins")
		.master("local[*]")
		.getOrCreate()

	val sc: SparkContext = spark.sparkContext

	/**
	 * An online store selling gaming laptops.
	 * 2 laptops are "similar" if they have the same make & model, but proc speed within 0.1
	 *
	 * For each laptop configuration, we are interested in the average sale price of "similar" models.
	 *
	 * . Acer Predator 2.9Ghz aylfaskjhrw
	 * . -> average sale price of all Acer Predators with CPU speed between 2.8 and 3.0 GHz
	 */

	val laptops: RDD[Laptop] = sc.parallelize(Seq.fill(40000)(DataGenerator.randomLaptop()))
	val laptopOffers: RDD[LaptopOffer] = sc.parallelize(Seq.fill(100000)(DataGenerator.randomLaptopOffer()))

	type Make = String
	type Model = String
	type Registration = String
	type ProcSpeed = Double
	type SalePrice = Double
	type AvgPrice = Double

	def noSkewJoin(): Long = {
		val preparedLaptops = laptops
			.flatMap { laptop =>
				Seq(
					laptop,
					laptop.copy(procSpeed = laptop.procSpeed - 0.1),
					laptop.copy(procSpeed = laptop.procSpeed + 0.1),
				)
			}
			.map {
				case Laptop(registration, make, model, procSpeed) => ((make, model, procSpeed), registration)
			}

		val preparedOffers = laptopOffers.map {
			case LaptopOffer(make, model, procSpeed, salePrice) => ((make, model, procSpeed), salePrice)
		}


		val prepLap_prepOffers: RDD[((Make, Model, ProcSpeed), (Registration, SalePrice))] = preparedLaptops.join(preparedOffers) // RDD[(make, model, procSpeed), (reg, salePrice))

		val result = prepLap_prepOffers
			.map(_._2)
			.aggregateByKey((0.0, 0))(
				{
					case ((totalPrice, numberOfPrices), salePrice) => (totalPrice + salePrice, numberOfPrices + 1) // combine state with record
				},
				{
					case ((totalPrices1, numberOfPrices1), (totalPrices2, numberOfPrices2)) => (totalPrices1 + totalPrices2, numberOfPrices1 + numberOfPrices2) // combine 2 states into one
				}
			) // RDD[(String, (Double, Int))]
			.mapValues {
				case (totalPrices, numberOfPrices) => totalPrices / numberOfPrices
			}

		result.count()
	}


	def main(args: Array[String]): Unit = {
		noSkewJoin()
		Thread.sleep(1000000)
	}
}