import cats.implicits._
import cats.effect._
import breeze._
import breeze.linalg._
import breeze.stats.distributions.{Gaussian, Pareto, Poisson, RandBasis, Uniform}
import org.apache.commons.math3.random.MersenneTwister
class DistributionsSpec extends munit .CatsEffectSuite {

  test("test"){
//    val u = Uniform(0,1)
//    val u = Gaussian(0,1)
//    val u = Poisson(1)
    val randBasis = new RandBasis(new MersenneTwister(1))
//RandBasis.withSeed(1)
    val u = Pareto(1,1)(randBasis)
//    (randBasis)
//    val uMean = u.mean
    val maxDownloads = 100
    val maxCustomers = 1
    val _maxIter = 20
    val maxIter = _maxIter*maxCustomers
    val sampleSize = 10
//    val uMode = u.mode
    val data = (0 until maxIter).map{ i =>
      val sample = u.sample(sampleSize).map(_.ceil)
        .map( x=> if(x>maxDownloads) maxDownloads else x)
        .sorted
//      println(sample)
      sample
    }.foldRight(DenseMatrix(List.empty[Double])) {
      case (values, matrix) =>
          DenseMatrix(( matrix.toArray.toList ++ values).sorted:_*)
    }
    val X = data.reshape(maxIter,sampleSize)
    val Y = sum(X(::,*))
//    *5.toDouble
    println(Y)
//    println(X(::,*))
//    println(sample.map(u.probabilityOf))
//    println(sample.map(u.pdf))
//    println(sample.map(u.cdf))
//    println(u.pdf(uMean))
//    println(u.sample(10))
//    println(u.cdf(u.sample))
//    println(u.mean)
  }

  test("test0"){
    for {
      _ <- IO.println("HELLO")
      randomGen    = new MersenneTwister(1)
      randomBas    = new RandBasis(randomGen)
      dist         = Pareto(scale= 1,shape = 1)(rand = randomBas)
      sample       = dist.sample(10).map(_.toInt)
      _            <- IO.println(sample.toString)
      sample       = dist.sample(10).map(_.toInt)
      _            <- IO.println(sample.toString)
    } yield 9
  }

}
