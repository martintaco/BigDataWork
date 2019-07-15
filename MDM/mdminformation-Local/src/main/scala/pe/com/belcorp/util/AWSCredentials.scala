package pe.com.belcorp.util

import com.amazonaws.auth.{ AWSSessionCredentials, InstanceProfileCredentialsProvider }

case class AWSCredentials(token: String, accessKey: String, secretKey: String)

object AWSCredentials {
  //filtrar desde abaajo para que no tenga acceso al S3
  /*def fetch(): AWSCredentials = {
    val provider = InstanceProfileCredentialsProvider.getInstance()
    val credentials = provider.getCredentials.asInstanceOf[AWSSessionCredentials]
    AWSCredentials(
      credentials.getSessionToken, credentials.getAWSAccessKeyId, credentials.getAWSSecretKey)
  }*/
}


