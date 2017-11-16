package azkaban.execapp.event;

import azkaban.utils.RestfulApiClient;
import com.google.gson.JsonObject;
import java.net.URI;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpResponseException;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;


public class StatusApiClient extends RestfulApiClient<String> {

  /**
   * Deserialize HttpResponse body
   */
  @Override
  protected String parseResponse(HttpResponse response)
      throws HttpResponseException, IOException {

    final StatusLine statusLine = response.getStatusLine();
    String body = response.getEntity() != null ?
      EntityUtils.toString(response.getEntity()) : "";

    if (statusLine.getStatusCode() != 200) {
      logger.error(String.format("Unexpected response status %s",
        statusLine.getStatusCode()));

      throw new HttpResponseException(statusLine.getStatusCode(), body);
    }
    return body;
  }

  /**
   * Wrapper for sending JSON POST request
   */
  public String postRequest(URI uri, JsonObject json)
      throws UnsupportedEncodingException, IOException {

    // Set HTTP header
    List<NameValuePair> headerEntries = new ArrayList <NameValuePair>();
    headerEntries.add(new BasicNameValuePair("Content-Type", "application/json"));

    // Send POST request
    String response = super.httpPost(uri, headerEntries, json.toString());
    return response;
  }
}