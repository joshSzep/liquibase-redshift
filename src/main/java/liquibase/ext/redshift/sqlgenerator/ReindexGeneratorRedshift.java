package liquibase.ext.redshift.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.ReindexStatement;
import liquibase.structure.core.Table;
import liquibase.ext.redshift.database.RedshiftDatabase;

public class ReindexGeneratorRedshift
        extends liquibase.sqlgenerator.core.AbstractSqlGenerator<ReindexStatement> {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(ReindexStatement statement,
                            Database database) {
        return (database instanceof RedshiftDatabase);
    }

    @Override
    public ValidationErrors validate(ReindexStatement reindexStatement,
                                     Database database,
                                     SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("tableName", reindexStatement.getTableName());
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(ReindexStatement statement,
                             Database database,
                             SqlGeneratorChain sqlGeneratorChain) {
        String catalog = statement.getCatalogName();
        String schema = statement.getSchemaName();
        String table = statement.getTableName();
        String escapedTableName = database.escapeTableName(catalog, schema, table);
        return new Sql[]{
                new UnparsedSql("REINDEX " + escapedTableName, new Table().setName(table).setSchema(catalog, schema))
        };
    }
}