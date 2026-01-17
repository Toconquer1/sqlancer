package sqlancer.oceanbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.oceanbase.gen.OceanBaseAlterTable;
import sqlancer.oceanbase.gen.OceanBaseDeleteGenerator;
import sqlancer.oceanbase.gen.OceanBaseDropIndex;
import sqlancer.oceanbase.gen.OceanBaseInsertGenerator;
import sqlancer.oceanbase.gen.OceanBaseTableGenerator;
import sqlancer.oceanbase.gen.OceanBaseTruncateTableGenerator;
import sqlancer.oceanbase.gen.OceanBaseUpdateGenerator;
import sqlancer.oceanbase.gen.datadef.OceanBaseIndexGenerator;

@AutoService(DatabaseProvider.class)
public class OceanBaseProvider extends SQLProviderAdapter<OceanBaseGlobalState, OceanBaseOptions> {

    public OceanBaseProvider() {
        super(OceanBaseGlobalState.class, OceanBaseOptions.class);
    }

    enum Action implements AbstractAction<OceanBaseGlobalState> {
        SHOW_TABLES((g) -> new SQLQueryAdapter("SHOW TABLES")), INSERT(OceanBaseInsertGenerator::insertRow),
        CREATE_INDEX(OceanBaseIndexGenerator::create), ALTER_TABLE(OceanBaseAlterTable::create),
        TRUNCATE_TABLE(OceanBaseTruncateTableGenerator::generate),
        SELECT_INFO((g) -> new SQLQueryAdapter(
                "select TABLE_NAME, ENGINE from information_schema.TABLES where table_schema = '" + g.getDatabaseName()
                        + "'")),
        CREATE_TABLE((g) -> {
            String tableName = DBMSCommon.createTableName(g.getSchema().getDatabaseTables().size());

            return OceanBaseTableGenerator.generate(g, tableName);
        }), DELETE(OceanBaseDeleteGenerator::delete), UPDATE(OceanBaseUpdateGenerator::update),
        DROP_INDEX(OceanBaseDropIndex::generate);

        private final SQLQueryProvider<OceanBaseGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<OceanBaseGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(OceanBaseGlobalState globalState) throws Exception {
            return sqlQueryProvider.getQuery(globalState);
        }
    }

    private static int mapActions(OceanBaseGlobalState globalState, Action a) {
        int nrPerformed = 0;
        switch (a) {
        case INSERT:
            nrPerformed = 30;
            break;
        default:
            nrPerformed = 0;
            break;
        }
        return nrPerformed;
    }

    @Override
    public void generateDatabase(OceanBaseGlobalState globalState) throws Exception {
        while (globalState.getSchema().getDatabaseTables().size() < 1) {
            String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
            SQLQueryAdapter createTable = OceanBaseTableGenerator.generate(globalState, tableName);
            globalState.executeStatement(createTable);
        }

        StatementExecutor<OceanBaseGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                OceanBaseProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    @Override
    public SQLConnection createDatabase(OceanBaseGlobalState globalState) throws Exception, SQLException {
        String username = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = OceanBaseOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = OceanBaseOptions.DEFAULT_PORT;
        }
        if (username.endsWith("sys") || username.equals("root")) {
            throw new OceanBaseUserCheckException(
                    "please don't use sys tenant to test! Firstly create tenant then test");
        }
        String databaseName = globalState.getDatabaseName();
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
        globalState.getState().logStatement("CREATE DATABASE " + databaseName);
        globalState.getState().logStatement("USE " + databaseName);
        String url = String.format("jdbc:mysql://%s:%d?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true",
                host, port);
        Connection con = DriverManager.getConnection(url, username, password);

        try (Statement s = con.createStatement()) {
            s.execute("set ob_query_timeout=" + globalState.getDbmsSpecificOptions().queryTimeout);
        }
        try (Statement s = con.createStatement()) {
            s.execute("set ob_trx_timeout=" + globalState.getDbmsSpecificOptions().trxTimeout);
        }
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("CREATE DATABASE " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("USE " + databaseName);
        }
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "oceanbase";
    }

}
