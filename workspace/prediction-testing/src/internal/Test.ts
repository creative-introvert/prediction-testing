import {createHash} from 'node:crypto';

import * as P from '../prelude.js';
import type {Classify, Label} from '../Classify.js';
import {
    LabelSchema,
    Stats,
    defaultIsEqual,
    defaultIsNil,
    makeClassify,
    precision,
    recall,
} from './Classify.js';
import type {
    Program,
    TestCase,
    TestSuite,
    TestResult,
    TestRunResults,
    Diff,
} from '../Test.js';
import type {TestRun} from '../Test.repository.js';
import {TestRepository} from '../Test.repository.js';

export const makeSha256 = <I>(input: I): string => {
    return createHash('sha256').update(JSON.stringify(input)).digest('hex');
};

const TestResult = {
    make: <I, O, T>(args: Omit<TestResult<I, O, T>, 'id' | 'hashTestCase'>) => {
        const {label, input, result, expected} = args;

        // Without `ordering`, in case the user decides to re-order test cases,
        // which shouldn't change the identity of the test result.
        // Without `tags`, as those are just for filtering.
        const id = makeSha256({label, input, result, expected});
        const hashTestCase = makeSha256({input, expected});

        return {
            ...args,
            id,
            hashTestCase,
        };
    },
};

export const TestResultSchema: P.Schema.Schema<TestResult> = P.Schema.Struct({
    id: P.Schema.String,
    hashTestCase: P.Schema.String,
    ordering: P.Schema.Int,
    input: P.Schema.Unknown,
    result: P.Schema.Unknown,
    expected: P.Schema.Unknown,
    label: LabelSchema,
    tags: P.Schema.Array(P.Schema.String),
});

const TestRunResults = {
    emptyFromTestRun: <I, O, T>(args: TestRun): TestRunResults<I, O, T> => ({
        ...args,
        testResultsByTestCaseHash: {},
        testCaseHashes: [],
        stats: Stats.empty(),
    }),
};

export const test = <I, O, T>({
    testCase: {input, expected, tags},
    program,
    classify,
    ordering,
}: {
    testCase: TestCase<I, T>;
    program: Program<I, O>;
    classify: Classify<O, T>;
    ordering: number;
}): P.Effect.Effect<TestResult<I, O, T>> => {
    return program(input).pipe(
        P.Effect.map(result =>
            TestResult.make({
                ordering,
                input,
                result,
                expected,
                tags: tags ?? [],
                label: classify(result, expected),
            }),
        ),
    );
};

export const all = <I, O, T>({
    testCases,
    program,
    classify = makeClassify(defaultIsEqual, defaultIsNil, defaultIsNil),
    name,
}: TestSuite<I, O, T>) =>
    P.Effect.gen(function* () {
        const currentTestRun =
            yield* TestRepository.getOrCreateCurrentTestRun(name);
        yield* TestRepository.clearTestRun(currentTestRun);

        let ordering = 0;
        return P.Stream.fromIterable(testCases).pipe(
            P.Stream.mapEffect(
                testCase =>
                    test({ordering: ordering++, testCase, program, classify}),
                {unordered: false},
            ),
            P.Stream.tap(testResult =>
                TestRepository.insertTestResult(testResult, name),
            ),
        );
    });

export const runCollectRecord =
    (testRun: TestRun) =>
    <I, O, T, E>(
        testResults$: P.Stream.Stream<TestResult<I, O, T>, E, TestRepository>,
    ): P.Effect.Effect<TestRunResults<I, O, T>, E, TestRepository> =>
        testResults$.pipe(
            P.Stream.runFoldEffect(
                TestRunResults.emptyFromTestRun<I, O, T>(testRun),
                (run, result) =>
                    P.Effect.gen(function* () {
                        if (
                            run.testResultsByTestCaseHash[
                                result.hashTestCase
                            ] !== undefined
                        ) {
                            yield* P.Effect.logWarning(
                                `Skipped duplicate test case. hashTestCase=${result.hashTestCase}`,
                            );
                            return run;
                        } else {
                            run.testResultsByTestCaseHash[result.hashTestCase] =
                                result;
                            run.testCaseHashes.push(result.hashTestCase);
                            run.stats[result.label]++;
                        }

                        return run;
                    }),
            ),
            P.Effect.map(run => {
                run.stats.precision = precision(run.stats);
                run.stats.recall = recall(run.stats);
                return run;
            }),
        );

export const diff = ({
    testRun: {stats},
    previousTestRun,
}: {
    testRun: TestRunResults;
    previousTestRun: P.Option.Option<TestRunResults>;
}): Diff =>
    previousTestRun.pipe(
        P.Option.match({
            onNone: () => stats,
            onSome: ({stats: previousStats}) => ({
                TP: stats.TP - previousStats.TP,
                TN: stats.TN - previousStats.TN,
                FP: stats.FP - previousStats.FP,
                FN: stats.FN - previousStats.FN,
                precision: stats.precision - previousStats.precision,
                recall: stats.recall - previousStats.recall,
            }),
        }),
    );
