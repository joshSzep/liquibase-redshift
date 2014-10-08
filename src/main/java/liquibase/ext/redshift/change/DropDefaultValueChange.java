package liquibase.ext.redshift.change;

import liquibase.change.ColumnConfig;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.ext.redshift.database.RedshiftDatabase;
import liquibase.statement.SqlStatement;
import liquibase.structure.core.Index;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Removes the default value from an existing column.
 */
@DatabaseChange(name="dropDefaultValue", description="Removes the database default value for a column", priority = 15, appliesTo = "column")
public class DropDefaultValueChange
        extends liquibase.change.core.DropDefaultValueChange {

    @Override
    public boolean supports(Database database) {
        return database instanceof RedshiftDatabase;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        if (database instanceof RedshiftDatabase) {
            return generateStatementsForRedshiftDatabase(database);
        }
        return super.generateStatements(database);
    }

    private SqlStatement[] generateStatementsForRedshiftDatabase(Database database) {
        List<SqlStatement> statements = new ArrayList<SqlStatement>();

        // define alter table logic
        RedshiftDatabase.AlterTableVisitor rename_alter_visitor = new RedshiftDatabase.AlterTableVisitor() {
            public ColumnConfig[] getColumnsToAdd() {
                return new ColumnConfig[0];
            }
            public boolean copyThisColumn(ColumnConfig column) {
                return true;
            }
            public boolean createThisColumn(ColumnConfig column) {
                if (column.getName().equals(getColumnName())) {
                    column.setDefaultValue(null);
                    column.setDefaultValueBoolean((Boolean) null);
                    column.setDefaultValueDate((Date) null);
                    column.setDefaultValueNumeric((Number)null);
                }
                return true;
            }
            public boolean createThisIndex(Index index) {
                return true;
            }
        };

        try {
            // alter table
            statements.addAll(RedshiftDatabase.getAlterTableStatements(
                    rename_alter_visitor,
                    database, getCatalogName(), getSchemaName(), getTableName()));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return statements.toArray(new SqlStatement[statements.size()]);
    }
}
