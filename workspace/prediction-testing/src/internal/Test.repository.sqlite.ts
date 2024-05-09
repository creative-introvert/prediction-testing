import * as Sql from '@effect/sql';
import * as Sqlite from '@effect/sql-sqlite-node';
import type {Fragment, Primitive} from '@effect/sql/Statement';

import * as P from '../prelude.js';
import type {
    TestRepository as _TestRepository,
    TestResultRead,
    TestRun,
} from '../Test.repository.js';
import {LabelSchema, StatsSchema} from './Classify.js';
import {split} from './lib/Schema.js';
import type {TestResult} from '../Test.js';

const tables = {
    testResults: 'test-results',
    testRuns: 'test-runs',
    testRunResults: 'test-run-results',
} as const;

const TestResultWriteSchema: P.Schema.Schema<TestResult, TestResultRead> =
    P.Schema.Struct({
        id: P.Schema.String,
        hashTestCase: P.Schema.String,
        ordering: P.Schema.Int,
        input: P.Schema.parseJson(P.Schema.Unknown),
        result: P.Schema.parseJson(P.Schema.Unknown),
        expected: P.Schema.parseJson(P.Schema.Unknown),
        label: LabelSchema,
        tags: split(','),
    });

const TestResultsSchema = TestResultWriteSchema.pipe(P.Schema.Array);

const TestRunReadSchema: P.Schema.Schema<TestRun> = P.Schema.Struct({
    id: P.Schema.Int,
    name: P.Schema.String,
    hash: P.Schema.String.pipe(P.Schema.NullOr),
});

const HasResultsSchema = P.Schema.Struct({
    count: P.Schema.Int,
}).pipe(P.Schema.Array);

const TestRunsReadSchema = TestRunReadSchema.pipe(P.Schema.Array);

export class TestRepository extends P.Effect.Tag('TestRunRepository')<
    _TestRepository,
    _TestRepository
>() {}

const makeTestRepository = P.Effect.gen(function* () {
    const sql = yield* Sql.client.Client;

    yield* sql`
        CREATE TABLE IF NOT EXISTS ${sql(tables.testRuns)}(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            hash TEXT,
            UNIQUE (id, name, hash)
        );
    `;

    yield* sql`CREATE INDEX IF NOT EXISTS idx_name_hash ON ${sql(tables.testRuns)}(name, hash)`;

    yield* sql`
        CREATE TABLE IF NOT EXISTS ${sql(tables.testResults)}(
            id TEXT PRIMARY KEY,
            hashTestCase TEXT NOT NULL,
            ordering INTEGER NOT NULL,
            input TEXT,
            result TEXT,
            expected TEXT,
            label TEXT NOT NULL,
            tags TEXT NOT NULL
        );
    `;

    yield* sql`
        CREATE TABLE IF NOT EXISTS ${sql(tables.testRunResults)}(
            testRun INTEGER NOT NULL,
            testResult TEXT NOT NULL,
            FOREIGN KEY (testRun) REFERENCES ${sql(tables.testRuns)}(id),
            FOREIGN KEY (testResult) REFERENCES ${sql(tables.testResults)}(id),
            UNIQUE (testRun, testResult)
        );
    `;

    const insertTestResult = (testResult: TestResult, name: string) =>
        P.Effect.gen(function* () {
            const encoded = yield* P.Schema.encode(TestResultWriteSchema)(
                testResult,
            );

            yield* sql`
                INSERT INTO ${sql(tables.testResults)}
                ${sql.insert(encoded)}
                ON CONFLICT(id) DO NOTHING;
            `;

            yield* sql`
                INSERT INTO ${sql(tables.testRunResults)}
                VALUES (
                    (
                        SELECT id FROM ${sql(tables.testRuns)}
                        WHERE hash IS NULL AND name = ${name}
                    ),
                    ${encoded.id}
                )
                ON CONFLICT(testRun, testResult) DO NOTHING;
            `;
        });

    const getTestResultsStream = (
        testRun: TestRun,
    ): P.Stream.Stream<
        TestResult,
        Sql.error.SqlError | P.Result.ParseError,
        never
    > =>
        P.Effect.gen(function* () {
            const raw = yield* sql<TestResultRead>`
                SELECT res.* FROM ${sql(tables.testResults)} res
                JOIN ${sql(tables.testRunResults)} runres ON res.id = runres.testResult
                JOIN ${sql(tables.testRuns)} runs ON runs.id = runres.testRun
                WHERE runs.id = ${testRun.id}
                ORDER BY ordering ASC;
            `;
            return yield* P.Schema.decode(TestResultsSchema)(raw);
        }).pipe(P.Stream.fromIterableEffect);

    const hasResults = (testRun: TestRun) =>
        P.Effect.gen(function* () {
            const b = yield* sql<{count: number}>`
                SELECT COUNT(*) count FROM ${sql(tables.testRunResults)}
                WHERE testRun = ${testRun.id};
            `;
            const r = yield* P.Schema.decode(HasResultsSchema)(b);
            return r[0].count > 0;
        });

    const getPreviousTestRun = (name: string) =>
        P.Effect.gen(function* () {
            const r = yield* sql<TestRun>`
                SELECT * FROM ${sql(tables.testRuns)}
                WHERE hash IS NOT NULL AND name = ${name}
                ORDER BY id DESC
                LIMIT 1;
            `;
            const p = yield* P.Schema.decode(TestRunsReadSchema)(r);
            return P.Array.head(p);
        });

    // const getPreviousTestResultsStream = (name: string) =>
    //     sql<TestResultRead>`
    //             SELECT res.* FROM ${sql(tables.testResults)} res
    //                 JOIN ${sql(tables.testRunResults)} runres ON res.hash = runres.testResult
    //                 JOIN ${sql(tables.testRuns)} runs ON runs.id = runres.testRun
    //             WHERE runs.hash IS NULL AND runs.name = ${name};
    //         `.stream.pipe(
    //         P.Stream.mapEffect(P.Schema.decode(TestResultSchema)),
    //     );

    const getAllTestRuns = P.Effect.gen(function* () {
        const results = yield* sql<TestRun>`
                SELECT * from ${sql(tables.testRuns)};
            `;

        return yield* P.Schema.decode(TestRunsReadSchema)(results);
    });

    const getAllTestResults = P.Effect.gen(function* () {
        const results = yield* sql<TestResultRead>`
                SELECT * from ${sql(tables.testResults)};
            `;

        return yield* P.Schema.decode(TestResultsSchema)(results);
    });

    const getOrCreateCurrentTestRun = (name: string) =>
        P.Effect.gen(function* () {
            const current = yield* sql<TestRun>`
                SELECT *
                FROM ${sql(tables.testRuns)}
                WHERE hash IS NULL AND name = ${name};
            `;

            if (current.length === 0) {
                return yield* sql<TestRun>`
                    INSERT INTO ${sql(tables.testRuns)}
                    ${sql.insert({name})}
                    RETURNING *;
                `;
            }

            return current;
        }).pipe(P.Effect.map(P.Array.unsafeGet(0)));

    const clearTestRun = (testRun: TestRun) =>
        P.Effect.gen(function* () {
            yield* sql`
                DELETE FROM ${sql(tables.testRunResults)}
                WHERE testRun = ${testRun.id};
            `;
        });

    const commitCurrentTestRun = ({name, hash}: {name: string; hash: string}) =>
        P.Effect.gen(function* () {
            // Transaction?
            yield* sql`
                UPDATE ${sql(tables.testRuns)}
                SET hash = ${hash}
                WHERE id = (
                    SELECT id
                    FROM ${sql(tables.testRuns)}
                    WHERE hash IS NULL AND name = ${name}
                );
            `;

            yield* sql`
                INSERT INTO ${sql(tables.testRuns)}
                ${sql.insert({name})};
            `;
        });

    const service: _TestRepository = {
        clearTestRun,
        commitCurrentTestRun,
        getAllTestResults,
        getAllTestRuns,
        getOrCreateCurrentTestRun,
        getPreviousTestRun,
        getTestResultsStream,
        hasResults,
        insertTestResult,
    };

    return service;
});

export const LiveLayer = P.Layer.effect(TestRepository, makeTestRepository);

export const makeSqliteLiveLayer = (dbPath: string) =>
    Sqlite.client.layer(
        P.Config.all({
            filename: P.Config.string(
                'PREDICTION_TESTING_SQLITE_FILENAME',
            ).pipe(P.Config.withDefault(dbPath)),
        }),
    );

export const SqliteTestLayer = Sqlite.client.layer(
    P.Config.all({
        filename: P.Config.string('PREDICTION_TESTING_SQLITE_FILENAME').pipe(
            P.Config.withDefault('test.db'),
        ),
    }),
);
