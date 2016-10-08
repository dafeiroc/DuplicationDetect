import com.dafei.tools.Utils;

import java.io.*;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-4-2
 * Time: 下午3:10
 * To change this template use File | Settings | File Templates.
 */
public class App {

    public static void computeAllPairHamming(String vectorFile) {
        Map<Integer, Integer> hammingfreq = new HashMap<Integer, Integer>();
        BufferedReader br = null;
        String line = null;
        String oneline[][] = new String[485][3];
        try {
            int i = 0;
            br = new BufferedReader(new FileReader(vectorFile));
            while ((line = br.readLine()) != null && i < 485) {
                String temp[] = line.split(",");
                oneline[i][0] = temp[0];
                oneline[i][1] = temp[1];
                oneline[i][2] = temp[2];
                i++;
            }
            br.close();
            File hamming_file = new File("e:\\hamming.txt");
            if(hamming_file.exists()) hamming_file.delete();
            BufferedWriter hamming_bw = new BufferedWriter(new FileWriter(hamming_file));
            for (int j = 0; j < 485; j++) {
                for (int k = j + 1; k < 485; k++) {

                    int dis = Utils.hammingDistance(oneline[j][2], oneline[k][2]);
                    hamming_bw.write(dis+" ");
                    hamming_bw.newLine();
                    if (!hammingfreq.containsKey(dis)) hammingfreq.put(dis, 1);
                    else hammingfreq.put(dis, hammingfreq.get(dis) + 1);
                    System.out.println("file: " + oneline[j][0] + " " + oneline[k][0] + " , hamming distance is: " + dis);
                    if (dis < 20) {
                        System.out.println("--------------- " + oneline[j][0] + " catch file: " + oneline[k][0]);
                    }

                }
            }
            hamming_bw.close();

            File w = new File("e:\\metadata_raw\\simhash_48bit_hamming.txt");
            if (w.exists()) w.delete();
            BufferedWriter bw = new BufferedWriter(new FileWriter(w));
            for (Map.Entry<Integer, Integer> entry : hammingfreq.entrySet()) {
                bw.write(entry.getKey() + "\t" + entry.getValue());
                bw.newLine();
            }
            bw.close();


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public static void main(String args[]) throws Exception {
        computeAllPairHamming("e:\\metadata_raw\\simhash_48bit.txt");


        System.out.println(getMD5("西安交通大学"));

    }
    public static String getMD5(String s) throws Exception{
        MessageDigest md = MessageDigest.getInstance("MD5");
        char[] ch = s.toCharArray();
        byte[] b = new byte[ch.length];
        for(int i=0;i<ch.length;i++)
             b[i] = (byte)ch[i] ;
        byte md5b[] = md.digest(b);
        StringBuffer sb = new StringBuffer();
        for(int i=0;i<md5b.length;i++){
            int val = ((int)md5b[i]) & 0xff;
            if(val<16) sb.append("0");
            sb.append(Integer.toHexString(val));
        }
        return sb.toString();
    }
}
