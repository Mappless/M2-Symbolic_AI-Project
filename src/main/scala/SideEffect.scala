import com.github.javafaker.Faker

class SideEffect {
  val faker = new Faker
  val sideEffectList =  List(
    ("pain", "C0151828"),
    ("fatigue", "C0015672"),
    ("headache", "C0018681"),
    ("Muscle pain", "C0231528"),
    ("chills", "C0085593"),
    ("Joint pain", "C0003862"),
    ("fever", "C0015967"),
    ("Injection site swelling", "C0151605"),
    ("Injection site redness", "C0852625"),
    ("Nausea", "C0027497"),
    ("Malaise", "C0231218"),
    ("Lymphadenopathy", "C0497156"),
    ("Injection site tenderness", "C0863083"))

  def test(n : Int):  (String, String) = {
    return sideEffectList(n)
  }

  def getRandomSideEffect(chanceOfSE : Float): (String, String) = {
    val chanceNumber = faker.number().numberBetween(0, 100)
    if (chanceNumber >= (chanceOfSE * 100).toInt)
      return null
    else
      return sideEffectList(faker.number().numberBetween(0, sideEffectList.size))
  }
}

object SideEffect {
  def apply(): SideEffect = new SideEffect()
}