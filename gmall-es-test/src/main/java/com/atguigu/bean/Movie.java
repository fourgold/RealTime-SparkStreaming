package com.atguigu.bean;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
/**
 * @author Jinxin Li
 * @create 2020-12-06 23:00
 */

public class Movie {

   private String id;
   private String movie;
   public Movie(String id,String movie){
      this.id=id;
      this.movie=movie;
   }

   public String getId() {
      return id;
   }

   public void setId(String id) {
      this.id = id;
   }

   public String getMovie() {
      return movie;
   }

   public void setMovie(String movie) {
      this.movie = movie;
   }
}
//创建一个javabean对象
//plugins创建一个插件
