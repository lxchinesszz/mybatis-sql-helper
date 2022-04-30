package mybatis.sql.helper.core.format;

import org.slf4j.helpers.MessageFormatter;
import org.springframework.boot.ansi.AnsiColor;
import org.springframework.boot.ansi.AnsiOutput;
import org.springframework.boot.ansi.AnsiStyle;

/**
 * @author liuxin
 * 2022/4/30 18:05
 */
public class ColorConsole {

    public static String color(String text, AnsiColor color) {
        AnsiOutput.setEnabled(AnsiOutput.Enabled.ALWAYS);
        return AnsiOutput.toString(color, text, AnsiColor.DEFAULT, AnsiStyle.BOLD);
    }

    public static String color(String text) {
        return color(text, AnsiColor.BRIGHT_GREEN);
    }

    public static void colorPrintln(String descFormat, Object... args) {
        System.out.println(color(MessageFormatter.arrayFormat(descFormat, args).getMessage()));
    }

    public static void colorPrintln(AnsiColor color, String descFormat, Object... args) {
        System.out.println(color(MessageFormatter.arrayFormat(descFormat, args).getMessage(), color));
    }
}
