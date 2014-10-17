package liquibase.ext.redshift.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.redshift.database.RedshiftDatabase;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.DropDefaultValueStatement;

public class DropDefaultValueGenerator extends
        liquibase.sqlgenerator.core.DropDefaultValueGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(DropDefaultValueStatement statement,
                            Database database) {
        return database instanceof RedshiftDatabase;
    }

    public ValidationErrors validate(DropDefaultValueStatement statement,
                                     Database database,
                                     SqlGeneratorChain sqlGeneratorChain) {
        return sqlGeneratorChain.validate(statement, database);
    }
}
