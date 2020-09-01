package evan.wang.etl.option;

/**
 * This class define commandline options for the Launcher program
 */
public class Options {
    @OptionRequired(description = "reader name")
    private String readerName;
    @OptionRequired(description = "writer name")
    private String writerName;
    @OptionRequired(description = "reader config, format by json")
    private String readerConfig;
    @OptionRequired(description = "writer config, format by json")
    private String writerConfig;

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

    public String getWriterConfig() {
        return writerConfig;
    }

    public void setWriterConfig(String writerConfig) {
        this.writerConfig = writerConfig;
    }
}
