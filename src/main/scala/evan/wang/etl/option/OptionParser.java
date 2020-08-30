
package evan.wang.etl.option;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Field;

/**
 * The Parser of Launcher commandline options
 */
public class OptionParser {

    private org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();

    private GnuParser parser = new GnuParser();

    private Options properties = new Options();

    public OptionParser(String[] args) throws Exception {
        initOptions(addOptions(args));
    }

    private CommandLine addOptions(String[] args) throws ParseException {
        Class cla = properties.getClass();
        Field[] fields = cla.getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            String name = field.getName();
            OptionRequired optionRequired = field.getAnnotation(OptionRequired.class);
            if (optionRequired != null) {
                options.addOption(name, optionRequired.hasArg(), optionRequired.description());
            }
        }
        return parser.parse(options, args);
    }

    private void initOptions(CommandLine cl) throws IllegalAccessException {
        Class cla = properties.getClass();
        Field[] fields = cla.getDeclaredFields();
        for (Field field : fields) {
            String name = field.getName();
            String value = cl.getOptionValue(name);
            OptionRequired optionRequired = field.getAnnotation(OptionRequired.class);
            if (optionRequired != null) {
                if (optionRequired.required() && StringUtils.isBlank(value)) {
                    throw new RuntimeException(String.format("parameters of %s is required", name));
                }
            }
            if (StringUtils.isNotBlank(value)) {
                field.setAccessible(true);
                field.set(properties, value);
            }
        }
    }

    public Options getOptions() {
        return properties;
    }

}
