import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class SaveFile {
    private static String PATH_PART = "./parse\\errors\\";

    public static void CreatDir(String brand) {
        File filePath = new File(PATH_PART + brand);
        if (!filePath.exists()) {
            filePath.mkdir();
        }
    }

//    public static void SaveData(String brand, String model, String data, Boolean append) {
    public static void SaveData(String dir, String fileName, String data, Boolean append) {
        File filePath = new File(dir);
        if (!filePath.exists()) {
            filePath.mkdir();
        }
        File file = new File(dir + fileName);
        FileWriter fw;
        try {
            fw = new FileWriter(file, append);
            fw.write(data + "\r\n");
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
