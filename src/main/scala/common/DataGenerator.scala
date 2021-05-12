package common

import scala.util.Random

object DataGenerator {

	val random = new Random()

	/////////////////////////////////////////////////////////////////////////////////
	// General data generation
	/////////////////////////////////////////////////////////////////////////////////

	def randomString(n: Int) =
		new String((0 to n).map(_ => ('a' + random.nextInt(26)).toChar).toArray)

	/////////////////////////////////////////////////////////////////////////////////
	// Laptop models generation - skewed data lectures
	/////////////////////////////////////////////////////////////////////////////////

	val laptopModelsSet: Seq[LaptopModel] = Seq(
		LaptopModel("Razer", "Blade"),
		LaptopModel("Alienware", "Area-51"),
		LaptopModel("HP", "Omen"),
		LaptopModel("Acer", "Predator"),
		LaptopModel("Asus", "ROG"),
		LaptopModel("Lenovo", "Legion"),
		LaptopModel("MSI", "Raider")
	)

	def randomLaptopModel(uniform: Boolean = false): LaptopModel = {
		val makeModelIndex = if (!uniform && random.nextBoolean()) 0 else random.nextInt(laptopModelsSet.size) // 50% of the data is of the first kind
		laptopModelsSet(makeModelIndex)
	}

	def randomProcSpeed(): Double = s"3.${random.nextInt(9)}".toDouble

	def randomRegistration(): String = s"${random.alphanumeric.take(7).mkString("")}"

	def randomPrice(): Int = 500 + random.nextInt(1500)

	def randomLaptop(uniformDist: Boolean = false): Laptop = {
		val makeModel = randomLaptopModel()
		Laptop(randomRegistration(), makeModel.make, makeModel.model, randomProcSpeed())
	}

	def randomLaptopOffer(uniformDist: Boolean = false): LaptopOffer = {
		val makeModel = randomLaptopModel()
		LaptopOffer(makeModel.make, makeModel.model, randomProcSpeed(), randomPrice())
	}

	/////////////////////////////////////////////////////////////////////////////////
	// Guitars generation - fixing skewed data lecture
	/////////////////////////////////////////////////////////////////////////////////

	val guitarModelSet: Seq[(String, String)] = Seq(
		("Taylor", "914"),
		("Martin", "D-18"),
		("Takamine", "P7D"),
		("Gibson", "L-00"),
		("Tanglewood", "TW45"),
		("Fender", "CD-280S"),
		("Yamaha", "LJ16BC")
	)

	/**
	 * Generates a tuple of (make, model) from the available set.
	 * If 'uniform' is false, it will have a 50% chance of picking one pair, and 50% chance for all the others.
	 */
	def randomGuitarModel(uniform: Boolean = false): (String, String) = {
		val makeModelIndex = if (!uniform && random.nextBoolean()) 0 else random.nextInt(guitarModelSet.size)
		guitarModelSet(makeModelIndex)
	}

	def randomSoundQuality(): Double = s"4.${random.nextInt(9)}".toDouble

	def randomGuitarRegistration(): String = randomString(8)

	def randomGuitarModelType(): String = s"${randomString(4)}-${randomString(4)}"

	def randomGuitarPrice(): Int = 500 + random.nextInt(1500)

	def randomGuitar(uniformDist: Boolean = false): Guitar = {
		val makeModel = randomGuitarModel(uniformDist)
		Guitar(randomGuitarModelType(), makeModel._1, makeModel._2, randomSoundQuality())
	}

	def randomGuitarSale(uniformDist: Boolean = false): GuitarSale = {
		val makeModel = randomGuitarModel(uniformDist)
		GuitarSale(randomGuitarRegistration(), makeModel._1, makeModel._2, randomSoundQuality(), randomGuitarPrice())
	}
}