package common

// Scenario: managing the data of a factory of handmade guitars like Taylor, Martin or Tanglewood
// Factories produce lots of guitars, and you can customize everything: wood types, decorations, materials, bracing etc
// Every instance of Guitar is a reference description of a guitar with certain features and a sound score from 0 to 5, considered "objective"
case class Guitar(
	                 configurationId: String,
	                 make: String,
	                 model: String,
	                 soundScore: Double
                 )

case class GuitarSale(
	                     registration: String,
	                     make: String,
	                     model: String,
	                     soundScore: Double,
	                     salePrice: Double
                     )