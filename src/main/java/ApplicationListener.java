import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

@WebListener
public class ApplicationListener implements ServletContextListener {
    private Consumer chatConsumer;
    private Thread consumerThread;

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        chatConsumer = new Consumer();
        consumerThread = new Thread(() -> chatConsumer.processEvents());
        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        if (chatConsumer != null) {
            chatConsumer.shutdown();
        }
        try {
            if (consumerThread != null) {
                consumerThread.join();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
