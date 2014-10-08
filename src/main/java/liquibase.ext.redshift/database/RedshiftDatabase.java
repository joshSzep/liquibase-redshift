package liquibase.ext.redshift.database;

import liquibase.CatalogAndSchema;
import liquibase.change.ColumnConfig;
import liquibase.change.core.CreateTableChange;
import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.database.core.PostgresDatabase;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotControl;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.*;
import liquibase.structure.core.*;
import liquibase.util.StringUtils;

import java.util.*;

public class RedshiftDatabase extends PostgresDatabase {

    private Set<String> redshiftReservedWords = new HashSet<String>();

    public RedshiftDatabase() {
        super.setCurrentDateTimeFunction("GETDATE()");

        redshiftReservedWords.addAll(Arrays.asList("AES128",
                "AES256",
                "ALL",
                "ALLOWOVERWRITE",
                "ANALYSE",
                "ANALYZE",
                "AND",
                "ANY",
                "ARRAY",
                "AS",
                "ASC",
                "AUTHORIZATION",
                "BACKUP",
                "BETWEEN",
                "BINARY",
                "BLANKSASNULL",
                "BOTH",
                "BYTEDICT",
                "CASE",
                "CAST",
                "CHECK",
                "COLLATE",
                "COLUMN",
                "CONSTRAINT",
                "CREATE",
                "CREDENTIALS",
                "CROSS",
                "CURRENT_DATE",
                "CURRENT_TIME",
                "CURRENT_TIMESTAMP",
                "CURRENT_USER",
                "CURRENT_USER_ID",
                "DEFAULT",
                "DEFERRABLE",
                "DEFLATE",
                "DEFRAG",
                "DELTA",
                "DELTA32K",
                "DESC",
                "DISABLE",
                "DISTINCT",
                "DO",
                "ELSE",
                "EMPTYASNULL",
                "ENABLE",
                "ENCODE",
                "ENCRYPT     ",
                "ENCRYPTION",
                "END",
                "EXCEPT",
                "EXPLICIT",
                "FALSE",
                "FOR",
                "FOREIGN",
                "FREEZE",
                "FROM",
                "FULL",
                "GLOBALDICT256",
                "GLOBALDICT64K",
                "GRANT",
                "GROUP",
                "GZIP",
                "HAVING",
                "IDENTITY",
                "IGNORE",
                "ILIKE",
                "IN",
                "INITIALLY",
                "INNER",
                "INTERSECT",
                "INTO",
                "IS",
                "ISNULL",
                "JOIN",
                "LEADING",
                "LEFT",
                "LIKE",
                "LIMIT",
                "LOCALTIME",
                "LOCALTIMESTAMP",
                "LUN",
                "LUNS",
                "LZO",
                "LZOP",
                "MINUS",
                "MOSTLY13",
                "MOSTLY32",
                "MOSTLY8",
                "NATURAL",
                "NEW",
                "NOT",
                "NOTNULL",
                "NULL",
                "NULLS",
                "OFF",
                "OFFLINE",
                "OFFSET",
                "OLD",
                "ON",
                "ONLY",
                "OPEN",
                "OR",
                "ORDER",
                "OUTER",
                "OVERLAPS",
                "PARALLEL",
                "PARTITION",
                "PERCENT",
                "PLACING",
                "PRIMARY",
                "RAW",
                "READRATIO",
                "RECOVER",
                "REFERENCES",
                "REJECTLOG",
                "RESORT",
                "RESTORE",
                "RIGHT",
                "SELECT",
                "SESSION_USER",
                "SIMILAR",
                "SOME",
                "SYSDATE",
                "SYSTEM",
                "TABLE",
                "TAG",
                "TDES",
                "TEXT255",
                "TEXT32K",
                "THEN",
                "TO",
                "TOP",
                "TRAILING",
                "TRUE",
                "TRUNCATECOLUMNS",
                "UNION",
                "UNIQUE",
                "USER",
                "USING",
                "VERBOSE",
                "WALLET",
                "WHEN",
                "WHERE",
                "WITH",
                "WITHOUT"));
    }

    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        return StringUtils.trimToEmpty(System.getProperty("liquibase.ext.redshift.force")).equalsIgnoreCase("true")
                || conn.getURL().contains(".redshift.")
                || conn.getURL().contains(":5439");
    }

    @Override
    public String getShortName() {
        return "redshift";
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return "Redshift";
    }

    @Override
    public boolean isReservedWord(String tableName) {
        if (super.isReservedWord(tableName)) {
            return true;
        }

        return redshiftReservedWords.contains(tableName.toUpperCase());

    }

    @Override
    public String getCurrentDateTimeFunction() {
        return "GETDATE()";
    }

    /*  Based on liquibase.database.core.SQLiteDatabase alter table pattern.
        Thankfully SQLite ALTER TABLE is/was limited in ways similar to
        redshift so a lot of existing code related to it can be leveraged
        for our purposes.

        Leveraged:
            AlterTableVisitor
            getAlterTableStatements

        See http://www.sqlite.org/lang_altertable.html for more info. */
    public interface AlterTableVisitor {
        public ColumnConfig[] getColumnsToAdd();

        public boolean copyThisColumn(ColumnConfig column);

        public boolean createThisColumn(ColumnConfig column);

        public boolean createThisIndex(Index index);
    }

    public static List<SqlStatement> getAlterTableStatements(
            AlterTableVisitor alterTableVisitor,
            Database database, String catalogName, String schemaName, String tableName)
            throws DatabaseException {

        List<SqlStatement> statements = new ArrayList<SqlStatement>();

        Table table;
        try {
            table = SnapshotGeneratorFactory.getInstance().createSnapshot((Table) new Table().setName(tableName).setSchema(new Schema(new Catalog(null), null)), database);

            List<ColumnConfig> createColumns = new ArrayList<ColumnConfig>();
            List<ColumnConfig> copyColumns = new ArrayList<ColumnConfig>();
            if (table != null) {
                for (Column column : table.getColumns()) {
                    ColumnConfig new_column = new ColumnConfig(column);
                    if (alterTableVisitor.createThisColumn(new_column)) {
                        createColumns.add(new_column);
                    }
                    ColumnConfig copy_column = new ColumnConfig(column);
                    if (alterTableVisitor.copyThisColumn(copy_column)) {
                        copyColumns.add(copy_column);
                    }

                }
            }
            for (ColumnConfig column : alterTableVisitor.getColumnsToAdd()) {
                if (alterTableVisitor.createThisColumn(column)) {
                    createColumns.add(column);
                }
                if (alterTableVisitor.copyThisColumn(column)) {
                    copyColumns.add(column);
                }
            }

            List<Index> newIndices = new ArrayList<Index>();
            for (Index index : SnapshotGeneratorFactory.getInstance().createSnapshot(new CatalogAndSchema(catalogName, schemaName), database, new SnapshotControl(database, Index.class)).get(Index.class)) {
                if (index.getTable().getName().equalsIgnoreCase(tableName)) {
                    if (alterTableVisitor.createThisIndex(index)) {
                        newIndices.add(index);
                    }
                }
            }

            // rename table
            String temp_table_name = tableName + "_temporary";

            statements.addAll(Arrays.asList(new RenameTableStatement(catalogName, schemaName, tableName, temp_table_name)));
            // create temporary table
            CreateTableChange ct_change_tmp = new CreateTableChange();
            ct_change_tmp.setSchemaName(schemaName);
            ct_change_tmp.setTableName(tableName);
            for (ColumnConfig column : createColumns) {
                ct_change_tmp.addColumn(column);
            }
            statements.addAll(Arrays.asList(ct_change_tmp.generateStatements(database)));
            // copy rows to temporary table
            statements.addAll(Arrays.asList(new CopyRowsStatement(temp_table_name, tableName, copyColumns)));
            // delete original table
            statements.addAll(Arrays.asList(new DropTableStatement(catalogName, schemaName, temp_table_name, false)));
            // validate indices
            statements.addAll(Arrays.asList(new ReindexStatement(catalogName, schemaName, tableName)));
            // add remaining indices
            for (Index index_config : newIndices) {
                statements.addAll(Arrays.asList(new CreateIndexStatement(
                        index_config.getName(),
                        catalogName, schemaName, tableName,
                        index_config.isUnique(),
                        index_config.getAssociatedWithAsString(),
                        index_config.getColumns().
                                toArray(new String[index_config.getColumns().size()]))));
            }

            return statements;
        } catch (InvalidExampleException e) {
            throw new UnexpectedLiquibaseException(e);
        }

    }
}
