package liquibase.ext.redshift.datatype;

import liquibase.database.Database;
import liquibase.datatype.DatabaseDataType;
import liquibase.ext.redshift.database.RedshiftDatabase;

public class DoubleTypeRedshift extends liquibase.datatype.core.DoubleType {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof RedshiftDatabase;
    }

    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {
        return new DatabaseDataType("FLOAT8");
    }

    @Override
    public String toString() {
        return "FLOAT8";
    }

}
