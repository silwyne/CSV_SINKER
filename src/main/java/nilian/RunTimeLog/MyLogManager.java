package nilian.RunTimeLog;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MyLogManager
{
    public static Logger getLogger(String loggerName)
    {
        Logger logger = Logger.getLogger(loggerName);
        // Create a console handler
        ConsoleHandler consoleHandler = new ConsoleHandler();
        consoleHandler.setFormatter(new MyLogFormatter());
        consoleHandler.setLevel(Level.ALL);
        logger.addHandler(consoleHandler);
        logger.setLevel(Level.ALL);
        logger.setUseParentHandlers(false);

        return logger;
    }
}