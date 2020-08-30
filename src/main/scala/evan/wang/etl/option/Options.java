package evan.wang.etl.option;

/**
 * This class define commandline options for the Launcher program
 */
public class Options {
    @OptionRequired(description = "Running mode")
    private String mode;
    @OptionRequired(description = "reader name")
    private String readerName;
    @OptionRequired(description = "writer name")
    private String writerName;
    @OptionRequired(description = "read config, format by json")
    private String readerConfig;
    @OptionRequired(description = "write config, format by json")
    private String writeConfig;

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getReaderName() {
        return readerName;
    }

    public void setReaderName(String readerName) {
        this.readerName = readerName;
    }

    public String getWriterName() {
        return writerName;
    }

    public void setWriterName(String writerName) {
        this.writerName = writerName;
    }

    public String getReaderConfig() {
        return readerConfig;
    }

    public void setReaderConfig(String readerConfig) {
        this.readerConfig = readerConfig;
    }

    public String getWriteConfig() {
        return writeConfig;
    }

    public void setWriteConfig(String writeConfig) {
        this.writeConfig = writeConfig;
    }
}
