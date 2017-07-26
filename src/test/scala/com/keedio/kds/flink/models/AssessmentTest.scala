package com.keedio.kds.flink.models

import org.junit.{Assert, Test}

/**
  * Created by luislazaro on 26/7/17.
  * lalazaro@keedio.com
  * Keedio
  */
class AssessmentTest {

  @Test
  def parseStringAsAssessmentTest() = {
    Assert.assertTrue(Assessment("TEST: This is a invalid comment,,,,;").isRight)
    Assert.assertTrue(Assessment("this is a valid comment,,,,,,,").isLeft)
    Assert.assertFalse(Assessment("TEST: This is a invalid comment,,,,;").isLeft)
    Assert.assertFalse(Assessment("this is a valid comment,,,,,,,").isRight)
  }

}
