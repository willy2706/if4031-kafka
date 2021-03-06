package if4031.client;

import if4031.client.command.*;

import java.io.PrintStream;
import java.util.List;
import java.util.Scanner;

public class CLInterface {

    private final Scanner scanner;
    private final PrintStream out;
    private final IRCClient ircClient;
    private final CommandParser commandParser = new CommandParser();

    CLInterface(Scanner _scanner, PrintStream _out, IRCClient _ircClient) {
        scanner = _scanner;
        out = _out;
        ircClient = _ircClient;
    }

    private void printMessages() {
        List<Message> messageList = ircClient.getMessages();
        if (messageList != null) {
            for (Message message : messageList) {
                out.println("[" + message.getChannel() + "] " + message.getBody());
            }
        }
    }

    void run() {
        // display welcome message
        out.println(WELCOME_MESSAGE);

        // main loop
        String commandString;
        while (true) {
            out.print(COMMAND_PROMPT);
            commandString = scanner.nextLine();
            CommandParser.ParseResult parseResult = commandParser.parse(commandString);

            CommandParser.ParseStatus status = parseResult.getStatus();
            if (status == CommandParser.ParseStatus.OK) {
                Command cmd = parseResult.getCommand();
                process(cmd);

            } else if (status == CommandParser.ParseStatus.EXIT) {
                break;
            }

            printMessages();
        }
    }

    void process(Command command) {
        if (command instanceof ChangeNicknameCommand) {
            ChangeNicknameCommand cmd = (ChangeNicknameCommand) command;
            String newNickname = cmd.getNewNickname();
            ircClient.changeNickname(newNickname);
            out.println("Nickname changed to " + newNickname);

        } else if (command instanceof JoinChannelCommand) {
            JoinChannelCommand cmd = (JoinChannelCommand) command;
            String channelName = cmd.getChannelName();
            ircClient.joinChannel(channelName);
            out.println("Joined channel: " + channelName);

        } else if (command instanceof LeaveChannelCommand) {
            LeaveChannelCommand cmd = (LeaveChannelCommand) command;
            String channelName = cmd.getChannelName();
            try {
                ircClient.leaveChannel(channelName);
                out.println("Left channel: " + channelName);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        } else if (command instanceof SendMessageAll) {
            SendMessageAll cmd = (SendMessageAll) command;
            ircClient.sendMessageAll(cmd.getMessage());
            out.println("Message sent");

        } else if (command instanceof SendMessageChannel) {
            SendMessageChannel cmd = (SendMessageChannel) command;
            ircClient.sendMessageChannel(cmd.getChannelName(), cmd.getMessage());
            out.println("Message sent");

        } else {
            // never happen
        }
    }

    private static String PROGRAM_NAME = "Apache Kafka - IRC";
    private static String WELCOME_MESSAGE = "Welcome to " + PROGRAM_NAME + "!\nChange your nickname with /nick command\n";
    private static String COMMAND_PROMPT = ">> ";
}
