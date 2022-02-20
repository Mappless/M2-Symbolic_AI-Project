import org.apache.jena.riot.{Lang, RDFDataMgr}

import java.io.FileOutputStream

object Main extends App {
  val lubm = Lubm(dbSource = "file:lubm1.ttl", "TTL")
  val persons = lubm.getPersons
  lubm.generateProperties(
    persons,
    .5F,
    Map(
      lubm.Vaccines.Pfizer -> 0.13F,
      lubm.Vaccines.Moderna -> 0.17F,
      lubm.Vaccines.SpoutnikV -> 0.4F,
      lubm.Vaccines.AstraZeneca -> 0.1F,
      lubm.Vaccines.CanSinoBio -> 0.2F
    )
  )

  //RDFDataMgr.write(new FileOutputStream("extendedLubm1.ttl"), lubm.model, Lang.TTL)
  //lubm.model.write(new FileOutputStream("test.xml"))
  lubm.loadOntology("file:univ-bench.owl")
}
