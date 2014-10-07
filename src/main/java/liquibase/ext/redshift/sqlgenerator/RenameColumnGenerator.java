package liquibase.ext.redshift.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.redshift.database.RedshiftDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.RenameColumnStatement;
import liquibase.structure.core.Column;
import liquibase.structure.core.Table;

public class RenameColumnGenerator extends
		liquibase.sqlgenerator.core.RenameColumnGenerator {

	@Override
	public int getPriority() {
		return 15;
	}

	public boolean supports(RenameColumnStatement statement, Database database) {
		return database instanceof RedshiftDatabase;
	}

	public ValidationErrors validate(RenameColumnStatement statement,
			Database database, SqlGeneratorChain sqlGeneratorChain) {
		return sqlGeneratorChain.validate(statement, database);
	}

	@Override
	public Sql[] generateSql(RenameColumnStatement statement,
			Database database, SqlGeneratorChain sqlGeneratorChain) {
		if (getAffectedOldColumn(statement).equals(
				getAffectedNewColumn(statement))
				&& statement.getRemarks() != null) {
			return new Sql[] { new UnparsedSql(
					"COMMENT ON COLUMN "
							+ database.escapeTableName(
									statement.getCatalogName(),
									statement.getSchemaName(),
									statement.getTableName())
							+ "."
							+ database.escapeColumnName(
									statement.getCatalogName(),
									statement.getSchemaName(),
									statement.getTableName(),
									statement.getNewColumnName())
							+ " IS '"
							+ database.escapeStringForDatabase(statement
									.getRemarks()) + "'",
					getAffectedColumn(statement)) };
		} else {
			return sqlGeneratorChain.generateSql(statement, database);
		}
	}

	protected Column getAffectedColumn(RenameColumnStatement statement) {
		return new Column().setName(statement.getNewColumnName()).setRelation(
				new Table().setName(statement.getTableName()).setSchema(
						statement.getCatalogName(), statement.getSchemaName()));
	}

}
