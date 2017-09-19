package azkaban.jobExecutor.loaders.utils;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.mockito.Mockito.*;


public class S3DownloaderTest {

  @Test
  public void testDownloader() throws Exception {
    S3FileDownloader downloader = new S3FileDownloader();

    ObjectMetadata mockResponse = mock(ObjectMetadata.class);
    AmazonS3Client client = mock(AmazonS3Client.class);
    downloader.setClient(client);

    String bucket = "someBucket";
    String key = "someKey.jar";
    String path = bucket + "/" + key;


    GetObjectRequest request = new GetObjectRequest(bucket, key);
    when(client.getObject(request, new File("/tmp/whatever"))).thenReturn(mockResponse);
    when(client.getObject(any(GetObjectRequest.class), any(File.class))).thenReturn(mockResponse);

    try {
      String outfile = downloader.download("s3://" + path, "/tmp/whatever");
      Assert.assertEquals(outfile, "/tmp/whatever");
    } catch (IOException e) {
      throw e;
    }
  }

}
