package sparksteam.steamsocket;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * 为steam的测试创建一个socket
 */
public class SocketForSteam {
    public static void main(String[] args) {
        ServerSocket server = null;
        try {
            server = new ServerSocket(9999);
            while (true) {
                Socket socket = server.accept();
                System.out.println("客户端:" + socket.getInetAddress().getLocalHost() + "已连接到服务器");
                BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
                String msg = in.readLine();

                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                bw.write(msg+"\n");
                bw.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(server!=null){
                try {
                    server.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
