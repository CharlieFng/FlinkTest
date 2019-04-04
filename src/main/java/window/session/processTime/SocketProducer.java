package window.session.processTime;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketProducer {
    public static void main(String[] args) throws IOException {

        ServerSocket listener = new ServerSocket(9090);
        try{
            Socket socket = listener.accept();
            System.out.println("Got new connection: " + socket.toString());

            ClassLoader classLoader = window.tumbling.processTime.SocketProducer.class.getClassLoader();
            File file = new File(classLoader.getResource("inputs/avg").getFile());
            InputStream inputStream = new FileInputStream(file);

            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

            try {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                String line;
                int count = 0;
                while ((line = br.readLine()) != null){
                    count++;

                    out.println(line);
                    if (count >= 10){
                        count = 0;
                        Thread.sleep(2000);
                    }
                    else
                        Thread.sleep(50);
                }

            } finally{
                socket.close();
            }

        } catch(Exception e ){
            e.printStackTrace();
        } finally{

            listener.close();
        }
    }

}
