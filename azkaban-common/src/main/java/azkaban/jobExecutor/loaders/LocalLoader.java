package azkaban.jobExecutor.loaders;

import azkaban.utils.Props;

public class LocalLoader extends DependencyLoader {

  @Override
  public String getDependency(String url) {
    return "";
  }

  public LocalLoader(Props props) {}

}
