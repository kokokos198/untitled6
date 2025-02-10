package org.example;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class Upload {

    public void upload_file(List<String> list, String path) throws IOException
    {
        String string1 = "operator_subj_dp_code,operator_subj_sk,sid,ts_gm_dp_code,ts_gm_name,ts_gm_sk";

        File file_new = new File(path);
        FileWriter fileWriter = new FileWriter(path, true);
        fileWriter.write(string1);
        fileWriter.write("\n");

        list.forEach(x -> {
            try {
                fileWriter.write(x);
                fileWriter.write("\n");
                //    fileWriter.close();
            } catch (IOException e) {
                System.out.println(e.getMessage());
                throw new RuntimeException(e);
            }
        });
        fileWriter.close();
    }


}
