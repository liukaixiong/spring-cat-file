package org.unidal.cat.message.bean;

public interface Tuple {
   public <T> T get(int index);

   public int size();
}
