package dev.oaiqiy;

import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class Output {
    public static void outputList(List<Tuple2<String,Double>> list, String path) throws IOException {

        File file = new File(path);

        FileOutputStream fileOutputStream = new FileOutputStream(file);

        for(Tuple2<String,Double> s : list){
            fileOutputStream.write((s._1 + "\t" + s._2 + "\n").getBytes(StandardCharsets.UTF_8));

        }

        fileOutputStream.close();

    }

}
