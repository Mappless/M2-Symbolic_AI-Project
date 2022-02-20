import com.github.javafaker.Faker
import org.apache.jena.assembler.ConstAssembler.ontModelSpec
import org.apache.jena.ontology.{OntModel, OntModelSpec}
import org.apache.jena.rdf.model.{Model, ModelFactory, Property, Resource, StmtIterator}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters._
import scala.math.floor
import scala.util.Random
import org.apache.jena.ontology.OntClass
import org.apache.jena.ontology.ObjectProperty
import org.apache.jena.riot.Lang

import java.io.FileOutputStream

class Lubm(val dbSource: String, val syntax: String) {
  val model: Model = ModelFactory.createDefaultModel().read(dbSource, syntax)
  val typeProperty: Property = model.getProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
  val faker = new Faker

  object PersonTypes {
    sealed trait PersonType {
      def resource: Resource
    }

    case object Lecturer extends PersonType {val resource: Resource = model.createResource("http://swat.cse.lehigh.edu/onto/univ-bench.owl#Lecturer")}
    case object UndergraduateStudent extends PersonType {val resource: Resource = model.createResource("http://swat.cse.lehigh.edu/onto/univ-bench.owl#UndergraduateStudent")}
    case object GraduateStudent extends PersonType {val resource: Resource = model.createResource("http://swat.cse.lehigh.edu/onto/univ-bench.owl#GraduateStudent")}
    case object AssociateProfessor extends PersonType {val resource: Resource = model.createResource("http://swat.cse.lehigh.edu/onto/univ-bench.owl#AssociateProfessor")}
    case object ResearchAssistant extends PersonType {val resource: Resource = model.createResource("http://swat.cse.lehigh.edu/onto/univ-bench.owl#ResearchAssistant")}
    case object TeachingAssistant extends PersonType {val resource: Resource = model.createResource("http://swat.cse.lehigh.edu/onto/univ-bench.owl#TeachingAssistant")}
    case object FullProfessor extends PersonType {val resource: Resource = model.createResource("http://swat.cse.lehigh.edu/onto/univ-bench.owl#FullProfessor")}
    case object AssistantProfessor extends PersonType {val resource: Resource = model.createResource("http://swat.cse.lehigh.edu/onto/univ-bench.owl#AssistantProfessor")}

    val personTypes: Array[PersonType] = Array[PersonType](Lecturer, UndergraduateStudent, GraduateStudent, AssociateProfessor, ResearchAssistant, TeachingAssistant, FullProfessor, AssistantProfessor)
    val youngTypes: Array[PersonType] = Array[PersonType](GraduateStudent, UndergraduateStudent, TeachingAssistant, AssistantProfessor, ResearchAssistant)

    object Properties {
      case object Id {val property: Property = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#id")}
      case object IsOfGender {val property: Property = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#gender")}
      case object FirstName {val property: Property = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#firstName")}
      case object LastName {val property: Property = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#lastName")}
      case object Address {val property: Property = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#address")}
      case object BirthDate {val property: Property = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#birthDate")}
      case object Vaccinated {val property: Property = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#vaccinated")}
    }
  }

  object Genders {
    val typeResource: Resource = model.createResource()

    sealed trait Gender {
      protected def generateResource(name: String): Resource = {
        val resource = model.createResource()
        resource.addProperty(Properties.GenderName.property, name)
        resource.addProperty(typeProperty, typeResource)
        resource
      }
    }

    case object Male extends Gender {val resource: Resource = generateResource("Male")}
    case object Female extends Gender {val resource: Resource = generateResource("Female")}

    object Properties {
      case object GenderName {val property: Property = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#name")}
    }
  }

  object Vaccines {
    val typeResource: Resource = model.createResource()

    sealed trait Vaccine {
      protected def generateResource(name: String): Resource = {
        val resource = model.createResource()
        resource.addProperty(Properties.VaccineName.property, name)
        resource.addProperty(typeProperty, typeResource)
        resource
      }
      def resource: Resource
    }

    case object Pfizer extends Vaccine {val resource: Resource = generateResource("Pfizer")}
    case object Moderna extends Vaccine {val resource: Resource = generateResource("Moderna")}
    case object AstraZeneca extends Vaccine {val resource: Resource = generateResource("AstraZeneca")}
    case object SpoutnikV extends Vaccine {val resource: Resource = generateResource("SpoutnikV")}
    case object CanSinoBio extends Vaccine {val resource: Resource = generateResource("CanSinoBio")}

    object Properties {
      case object VaccineName {val property: Property = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#name")}
    }
  }

  case class Vaccination(vaccine: Vaccines.Vaccine, vaccinationDate: LocalDate) {
    val resource: Resource = {
      val vaccination = model.createResource()
      vaccination.addProperty(typeProperty, Vaccination.typeResource)
      vaccination.addProperty(Vaccination.Properties.VaccineType.property, vaccine.resource)
      vaccination.addProperty(Vaccination.Properties.VaccinationDate.property, vaccinationDate.format(DateTimeFormatter.ISO_LOCAL_DATE))
      vaccination
    }
  }
  object Vaccination {
    val typeResource: Resource = model.createResource()

    object Properties {
      case object VaccineType {val property: Property = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#vaccineType")}
      case object VaccinationDate {val property: Property = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#vaccinationDate")}
    }
  }

  def showModel(): Unit = if (model.isEmpty) println("Model is empty") else println(model)

  def size: Long = model.size()

  def getTypes: List[String] = {
    val list = model.listStatements(null, typeProperty, null)
    list.asScala.map(statement => statement.getObject.toString).toList
  }

  def getPersons: Map[PersonTypes.PersonType, List[Resource]] = {
    PersonTypes.personTypes.map {
      personType =>
        val statements: StmtIterator = model.listStatements(null, typeProperty, personType.resource)
        personType -> statements.asScala.map(statement => statement.getSubject).toList
    }.toMap
  }

  def generateNotConfigurableProperties(persons: Map[PersonTypes.PersonType, List[Resource]]): Map[PersonTypes.PersonType, List[Resource]] = {
    generateID(persons.values.flatten.toList.distinct)
    generateCoordinates(persons.values.flatten.toList.distinct)
    generateBirthDate(persons)
  }

  def generateProperties(persons: Map[PersonTypes.PersonType, List[Resource]], malePercentage: Float, vaccinationPercents: Map[Vaccines.Vaccine, Float]): Unit = {
    generateNotConfigurableProperties(persons)
    generateGender(persons.values.flatten.toList.distinct, malePercentage)
    vaccinateEveryone(persons.values.flatten.toList.distinct, vaccinationPercents)
  }

  private def generateID(persons: List[Resource]): List[Resource] = {
    persons.zipWithIndex.foreach{ case(person, id) => person.addProperty(PersonTypes.Properties.Id.property, id.toString)}
    persons
  }

  private def generateCoordinates(persons: List[Resource]): List[Resource] = {
    persons.foreach(
      person => {
        person.addProperty(PersonTypes.Properties.FirstName.property, faker.name.firstName)
        person.addProperty(PersonTypes.Properties.LastName.property, faker.name.lastName)
        person.addProperty(PersonTypes.Properties.Address.property, faker.address.zipCode)
      }
    )
    persons
  }

  private def generateGender(persons: List[Resource], malePercent: Float): List[Resource] = {
    val sortedPerson = Random.shuffle(persons.distinct)

    val (male, female) = sortedPerson.splitAt((persons.size * malePercent).toInt)

    male.foreach(person => person.addProperty(PersonTypes.Properties.IsOfGender.property, Genders.Male.resource))
    female.foreach(person => person.addProperty(PersonTypes.Properties.IsOfGender.property, Genders.Female.resource))

    persons
  }

  private def generateBirthDate(persons: Map[PersonTypes.PersonType, List[Resource]]): Map[PersonTypes.PersonType, List[Resource]] = {
    val oldPersons = persons.filter{case (personType, _) => !PersonTypes.youngTypes.contains(personType)}.values.flatten.toList.distinct
    val youngPersons = persons.filter{case (personType, _) => PersonTypes.youngTypes.contains(personType)}.values.flatten.toList.distinct

    oldPersons.foreach(
      person => {
        person.addProperty(
          PersonTypes.Properties.BirthDate.property,
          LocalDate.now()
            .minusYears(30 + Random.nextInt(40))
            .minusDays(Random.nextInt(365))
            .format(DateTimeFormatter.ISO_LOCAL_DATE))
      }
    )
    youngPersons.foreach(
      person => {
        person.addProperty(
          PersonTypes.Properties.BirthDate.property,
          LocalDate.now()
            .minusYears(20 + Random.nextInt(10))
            .minusDays(Random.nextInt(365))
            .format(DateTimeFormatter.ISO_LOCAL_DATE))
      }
    )

    persons
  }

  def vaccinateEveryone(persons: List[Resource], vaccinesPercents: Map[Vaccines.Vaccine, Float]): Unit = {
    val intVaccinePercents = vaccinesPercents.map{case(vaccine, percent) => vaccine -> floor(percent * 100).toInt}
    val vaccines = intVaccinePercents.map{case(vaccine, percent) => List.fill(percent * persons.size)(vaccine)}.toList.flatten
    val sortedPersons = Random.shuffle(persons.take(vaccines.size))
    val sortedVaccines = Random.shuffle(vaccines)
    val personAndVaccine = sortedPersons.zip(sortedVaccines)

    personAndVaccine.foreach{
      case(person, vaccine) =>
        person.addProperty(
          PersonTypes.Properties.Vaccinated.property,
          Vaccination(
            vaccine,
            LocalDate.now().minusDays(Random.nextInt(365 * 2))
          ).resource)
    }
  }

  def loadOntology(owlSource : String) = {
    //if (model == null) throw new IllegalArgumentException("Model cannot be null");

    // Load Ontology source
    val NS: String = "" //owlSource + "#"
    val ontModelSpec = OntModelSpec.OWL_MEM
    val baseModel = ModelFactory.createOntologyModel(ontModelSpec)
    baseModel.read(owlSource);

    // TODO Add les nouvelles classes et sous classes: Vaccin, properties, Gender ...
    // TODO Add les properties entre les classes
    /*
    //Person's properties object
    val person = baseModel.getOntClass(NS + "Person")
    val id = baseModel.createClass(NS + "Id")
    val isOfGender = baseModel.createClass(NS + "IsOfGender")
    val firstName = baseModel.createClass(NS + "FirstName")
    val lastName = baseModel.createClass(NS + "LastName")
    val address = baseModel.createClass(NS + "Address")
    val birthDate = baseModel.createClass(NS + "BirthDate")
    val vaccinated = baseModel.createClass(NS + "Vaccinated")

    val hasId = baseModel.createObjectProperty(NS + "hasId")
    val hasGender = baseModel.createObjectProperty(NS + "hasGender")
    val hasFirstName = baseModel.createObjectProperty(NS + "hasFirstName")
    val hasLastName = baseModel.createObjectProperty(NS + "hasLastName")
    val hasAddress = baseModel.createObjectProperty(NS + "hasAddress")
    val hasBirthDate = baseModel.createObjectProperty(NS + "hasBirthDate")
    val hasVaccinated = baseModel.createObjectProperty(NS + "hasVaccinated")

    println("hasID is null : " + hasId == null)

    hasId.addDomain(person)
    hasGender.addDomain(person)
    hasFirstName.addDomain(person)
    hasLastName.addDomain(person)
    hasAddress.addDomain(person)
    hasBirthDate.addDomain(person)
    hasVaccinated.addDomain(person)

    hasId.addRange(id)
    hasGender.addRange(isOfGender)
    hasFirstName.addRange(firstName)
    hasLastName.addRange(lastName)
    hasAddress.addRange(address)
    hasBirthDate.addRange(birthDate)
    hasVaccinated.addRange(vaccinated)

     */

    //val inf = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM_RDFS_INF, baseModel);
    baseModel.write(new FileOutputStream("extended-univ-bench.owl"))

    /*
        val vaccine = baseModel.createClass(NS + "Vaccine")
        val Pfizer = baseModel.createClass(NS + "Pfizer")
        val Moderna = baseModel.createClass(NS + "Moderna")
        val AstraZeneca = baseModel.createClass(NS + "AstraZeneca")
        val SpoutnikV = baseModel.createClass(NS + "SpoutnikV")
        val CanSinoBio = baseModel.createClass(NS + "CanSinoBio")

        val vaccination = baseModel.createClass(NS + "Vaccination")

        val gender = baseModel.createClass(NS + "Gender")
        val male = baseModel.createClass(NS + "Male")
        val female = baseModel.createClass(NS + "Female")

     */






    //case object Id {val property: Property = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#id")}
    //case object IsOfGender {val property: Property = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#gender")}
    //case object FirstName {val property: Property = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#firstName")}
    //case object LastName {val property: Property = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#lastName")}
    //case object Address {val property: Property = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#address")}
    //case object BirthDate {val property: Property = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#birthDate")}
    //case object Vaccinated {val property: Property = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#vaccinated")}

    //Val programme = tempModel.createClass( NS + "Programme" );
    //Val orgEvent = tempModel.createClass( NS + "OrganizedEvent" );

    //Return the ontology
    //println(inf) // but here I println this




  }
}
object Lubm {
  def apply(dbSource: String, syntax: String): Lubm = new Lubm(dbSource, syntax)
}
