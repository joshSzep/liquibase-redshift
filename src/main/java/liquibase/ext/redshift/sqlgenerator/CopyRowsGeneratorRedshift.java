package liquibase.ext.redshift.sqlgenerator;

import liquibase.change.ColumnConfig;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.CopyRowsStatement;
import liquibase.structure.core.Relation;
import liquibase.structure.core.Table;
import liquibase.ext.redshift.database.RedshiftDatabase;

import java.lang.Override;

public class CopyRowsGeneratorRedshift
        extends liquibase.sqlgenerator.core.CopyRowsGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }


    @Override
    public boolean supports(CopyRowsStatement statement, Database database) {
        return (database instanceof RedshiftDatabase);
    }

    @Override
    public ValidationErrors validate(CopyRowsStatement copyRowsStatement,
                                     Database database,
                                     SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("targetTable", copyRowsStatement.getTargetTable());
        validationErrors.checkRequiredField("sourceTable", copyRowsStatement.getSourceTable());
        validationErrors.checkRequiredField("copyColumns", copyRowsStatement.getCopyColumns());
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(CopyRowsStatement statement,
                             Database database,
                             SqlGeneratorChain sqlGeneratorChain) {
        StringBuffer sql = new StringBuffer();
        if (database instanceof RedshiftDatabase) {
            sql.append("INSERT INTO ").append(statement.getTargetTable()).append(" (");

            for (int i = 0; i < statement.getCopyColumns().size(); i++) {
                ColumnConfig column = statement.getCopyColumns().get(i);
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append(column.getName());
            }

            sql.append(") SELECT ");
            for (int i = 0; i < statement.getCopyColumns().size(); i++) {
                ColumnConfig column = statement.getCopyColumns().get(i);
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append(column.getName());
            }
            sql.append(" FROM ").append(statement.getSourceTable());
        }

        return new Sql[]{
                new UnparsedSql(sql.toString(), getAffectedTable(statement))
        };
    }

    protected Relation getAffectedTable(CopyRowsStatement statement) {
        return new Table().setName(statement.getTargetTable()).setSchema(null, null);
    }
}
