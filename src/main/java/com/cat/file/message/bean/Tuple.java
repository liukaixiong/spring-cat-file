package com.cat.file.message.bean;

public interface Tuple {
   public <T> T get(int index);

   public int size();
}
