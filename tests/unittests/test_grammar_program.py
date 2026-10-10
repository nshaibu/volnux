import unittest

from volnux.parser.grammar import pointy_parser
from volnux.parser.ast import (
    BinOpNode,
    ConditionalNode,
    TaskNode,
    MetaTaskNode,
    MapNode,
    TernaryExprNode,
    IndexExprNode,
    VariableAccessNode,
    ListNode,
)


class TestProgram(unittest.TestCase):
    def test_empty_program(self):
        program = pointy_parser("")
        self.assertIsNotNone(program)
        self.assertEqual(program.directives, {})
        self.assertEqual(program.global_variables, {})
        self.assertIsNone(program.chain)

    def test_program_with_directives_and_variables(self):
        program = pointy_parser('@mode:"CFG" @version:1.0 @foo=42')
        self.assertIn("mode", program.directives)
        self.assertIn("version", program.directives)
        self.assertIn("foo", program.global_variables)
        self.assertEqual(program.directives["mode"].value, "CFG")
        self.assertEqual(program.directives["version"].value, 1.0)
        self.assertEqual(program.global_variables["foo"].value, 42)

    def test_program_with_chain(self):
        program = pointy_parser("TaskA -> TaskB")
        self.assertIsNotNone(program.chain)
        self.assertEqual(program.chain.op, "->")
        self.assertEqual(program.chain.left.task, "TaskA")
        self.assertEqual(program.chain.right.task, "TaskB")

    def test_program_with_directives_variables_and_chain(self):
        program = pointy_parser('@mode:"CFG" @foo=42 TaskA -> TaskB')
        self.assertIn("mode", program.directives)
        self.assertIn("foo", program.global_variables)
        self.assertEqual(program.directives["mode"].value, "CFG")
        self.assertEqual(program.global_variables["foo"].value, 42)
        self.assertIsNotNone(program.chain)
        self.assertEqual(program.chain.op, "->")
        self.assertEqual(program.chain.left.task, "TaskA")
        self.assertEqual(program.chain.right.task, "TaskB")

    def test_simple_conditional_chain_with_directive(self):
        program = pointy_parser(
            """
            @mode:"CFG"
            
            StartProcess -> EvaluateCondition (
                0 -> EndProcess,
                1 -> PerformWork -> EvaluateCondition  # Cycle back
            )
            """
        )

        self.assertIn("mode", program.directives)
        self.assertEqual(program.directives["mode"].value, "CFG")
        self.assertIsNotNone(program.chain)
        self.assertEqual(program.chain.op, "->")
        self.assertEqual(program.chain.left.task, "StartProcess")
        conditional = program.chain.right
        self.assertEqual(conditional.task.task, "EvaluateCondition")
        self.assertEqual(len(conditional.branches), 2)
        branches = {a.condition.value: a for a in conditional.branches}
        self.assertEqual(branches[0].operator, "->")
        self.assertEqual(branches[0].task.task, "EndProcess")
        self.assertEqual(branches[1].operator, "->")
        self.assertIsInstance(branches[1].task, BinOpNode)
        self.assertEqual(branches[1].task.op, "->")
        self.assertEqual(branches[1].task.left.task, "PerformWork")
        self.assertEqual(branches[1].task.right.task, "EvaluateCondition")

    def test_nested_conditional_workflow_with_directives(self):
        program = pointy_parser(
            """
                @mode:"CFG"
                @recursive_depth:5000
                
                StartJob -> ProcessBatch (
                    0 -> LogError -> NotifyAdmin,
                    1 -> ValidateResults (
                        0 -> CorrectData -> ProcessBatch,  # Loop back for retry
                        1 -> FinalizeJob
                    )
                )
            """
        )

        self.assertIn("mode", program.directives)
        self.assertIn("recursive_depth", program.directives)
        self.assertEqual(program.directives["mode"].value, "CFG")
        self.assertEqual(program.directives["recursive_depth"].value, 5000)

        self.assertIsNotNone(program.chain)
        self.assertEqual(program.chain.op, "->")
        self.assertEqual(program.chain.left.task, "StartJob")
        conditional = program.chain.right
        self.assertIsInstance(conditional, ConditionalNode)
        self.assertEqual(conditional.task.task, "ProcessBatch")
        self.assertEqual(len(conditional.branches), 2)
        branches = {a.condition.value: a for a in conditional.branches}

        # Branch 0
        self.assertEqual(branches[0].operator, "->")
        self.assertIsInstance(branches[0].task, BinOpNode)
        self.assertEqual(branches[0].task.op, "->")
        self.assertEqual(branches[0].task.left.task, "LogError")
        self.assertEqual(branches[0].task.right.task, "NotifyAdmin")

        # Branch 1
        self.assertEqual(branches[1].operator, "->")
        self.assertIsInstance(branches[1].task, ConditionalNode)
        nested_conditional = branches[1].task
        self.assertEqual(nested_conditional.task.task, "ValidateResults")
        self.assertEqual(len(nested_conditional.branches), 2)
        nested_branches = {a.condition.value: a for a in nested_conditional.branches}

        # Nested Branch 0
        self.assertEqual(nested_branches[0].operator, "->")
        self.assertIsInstance(nested_branches[0].task, BinOpNode)
        self.assertEqual(nested_branches[0].task.op, "->")
        self.assertEqual(nested_branches[0].task.left.task, "CorrectData")
        self.assertEqual(nested_branches[0].task.right.task, "ProcessBatch")

        # Nested Branch 1
        self.assertEqual(nested_branches[1].operator, "->")
        self.assertIsInstance(nested_branches[1].task, TaskNode)
        self.assertEqual(nested_branches[1].task.task, "FinalizeJob")

    def test_triple_nested_conditional_workflow_with_directives(self):
        program = pointy_parser(
            """
            @mode:"DAG"
            @recursive_depth:3000
            
            Level1 -> Level2 (
                0 -> ErrorPath1 -> ErrorPath2 -> ErrorPath3,
                1 -> Level3 (
                    0 -> FallbackA -> FallbackB,
                    1 -> Level4 (
                        0 -> RecoveryFlow,
                        1 -> Level5 -> Level6 -> Level7
                    )
                )
            )
            """
        )

        self.assertIn("mode", program.directives)
        self.assertIn("recursive_depth", program.directives)
        self.assertEqual(program.directives["mode"].value, "DAG")
        self.assertEqual(program.directives["recursive_depth"].value, 3000)

        self.assertIsNotNone(program.chain)
        self.assertEqual(program.chain.op, "->")
        self.assertEqual(program.chain.left.task, "Level1")
        conditional = program.chain.right
        self.assertIsInstance(conditional, ConditionalNode)
        self.assertEqual(conditional.task.task, "Level2")
        self.assertEqual(len(conditional.branches), 2)
        branches = {a.condition.value: a for a in conditional.branches}

        # Branch 0
        self.assertEqual(branches[0].operator, "->")
        self.assertIsInstance(branches[0].task, BinOpNode)
        self.assertEqual(branches[0].task.op, "->")
        self.assertIsInstance(branches[0].task.left, BinOpNode)
        self.assertIsInstance(branches[0].task.left.left, TaskNode)
        self.assertEqual(branches[0].task.left.left.task, "ErrorPath1")
        self.assertIsInstance(branches[0].task.left.right, TaskNode)
        self.assertEqual(branches[0].task.left.right.task, "ErrorPath2")
        self.assertIsInstance(branches[0].task.right, TaskNode)
        self.assertEqual(branches[0].task.right.task, "ErrorPath3")

        # Branch 1
        self.assertEqual(branches[1].operator, "->")
        self.assertIsInstance(branches[1].task, ConditionalNode)
        level3_conditional = branches[1].task
        self.assertEqual(level3_conditional.task.task, "Level3")
        self.assertEqual(len(level3_conditional.branches), 2)
        level3_branches = {a.condition.value: a for a in level3_conditional.branches}

        # Level3 Branch 0
        self.assertEqual(level3_branches[0].operator, "->")
        self.assertIsInstance(level3_branches[0].task, BinOpNode)
        self.assertEqual(level3_branches[0].task.op, "->")
        self.assertEqual(level3_branches[0].task.left.task, "FallbackA")
        self.assertEqual(level3_branches[0].task.right.task, "FallbackB")

        # Level3 Branch 1
        self.assertEqual(level3_branches[1].operator, "->")
        self.assertIsInstance(level3_branches[1].task, ConditionalNode)
        level4_conditional = level3_branches[1].task
        self.assertEqual(level4_conditional.task.task, "Level4")
        self.assertEqual(len(level4_conditional.branches), 2)
        level4_branches = {a.condition.value: a for a in level4_conditional.branches}

        # Level4 Branch 0
        self.assertEqual(level4_branches[0].operator, "->")
        self.assertIsInstance(level4_branches[0].task, TaskNode)
        self.assertEqual(level4_branches[0].task.task, "RecoveryFlow")

        # Level4 Branch 1
        self.assertEqual(level4_branches[1].operator, "->")
        self.assertIsInstance(level4_branches[1].task, BinOpNode)
        self.assertEqual(level4_branches[1].task.op, "->")
        self.assertIsInstance(level4_branches[1].task.left, BinOpNode)
        self.assertEqual(level4_branches[1].task.left.op, "->")
        self.assertIsInstance(level4_branches[1].task.left.left, TaskNode)
        self.assertEqual(level4_branches[1].task.left.left.task, "Level5")
        self.assertIsInstance(level4_branches[1].task.left.right, TaskNode)
        self.assertEqual(level4_branches[1].task.left.right.task, "Level6")
        self.assertIsInstance(level4_branches[1].task.right, TaskNode)
        self.assertEqual(level4_branches[1].task.right.task, "Level7")

    def test_multi_namespace_workflow_with_directives(self):
        program = pointy_parser(
            """
            @mode:"DAG"
            @recursive_depth:1500
            
            local::FetchOrders 
                -> pypi::ValidateOrders 
                -> github::EnrichWithCustomerData || local::CalculateTotals |-> pypi::GenerateInvoice
            """
        )

        self.assertIn("mode", program.directives)
        self.assertIn("recursive_depth", program.directives)
        self.assertEqual(program.directives["mode"].value, "DAG")
        self.assertEqual(program.directives["recursive_depth"].value, 1500)

        self.assertIsNotNone(program.chain)
        # With equal precedence all operators are left-associative, final top op becomes '|->'
        self.assertEqual(program.chain.op, "|->")
        right = program.chain.right
        self.assertIsInstance(right, TaskNode)
        self.assertEqual(right.namespace, "pypi")
        self.assertEqual(right.task, "GenerateInvoice")

        left = program.chain.left
        self.assertIsInstance(left, BinOpNode)
        self.assertEqual(left.op, "||")

        # right side of the '||' should be the local::CalculateTotals task
        self.assertIsInstance(left.right, TaskNode)
        self.assertEqual(left.right.namespace, "local")
        self.assertEqual(left.right.task, "CalculateTotals")

        # left side of the '||' is the chained '->' operations: ((FetchOrders -> ValidateOrders) -> EnrichWithCustomerData)
        self.assertIsInstance(left.left, BinOpNode)
        self.assertEqual(left.left.op, "->")

        # outer '->' has right = github::EnrichWithCustomerData
        self.assertIsInstance(left.left.right, TaskNode)
        self.assertEqual(left.left.right.namespace, "github")
        self.assertEqual(left.left.right.task, "EnrichWithCustomerData")

        # outer '->' has left = inner '->' (FetchOrders -> ValidateOrders)
        self.assertIsInstance(left.left.left, BinOpNode)
        self.assertEqual(left.left.left.op, "->")
        self.assertIsInstance(left.left.left.left, TaskNode)
        self.assertEqual(left.left.left.left.namespace, "local")
        self.assertEqual(left.left.left.left.task, "FetchOrders")
        self.assertIsInstance(left.left.left.right, TaskNode)
        self.assertEqual(left.left.left.right.namespace, "pypi")
        self.assertEqual(left.left.left.right.task, "ValidateOrders")

    def test_variables_with_meta_events(self):
        program = pointy_parser(
            """
            @parallel_workers = 5
            @batch_config = {"size": 100, "timeout": 30}
            @enable_retry = true
            
            ConfigureSystem[worker_count=$parallel_workers] ->
            FetchWorkQueue |->
            MAP<ProcessWorkItem>[
                batch_size=$batch_config["size"],
                concurrent=true,
                retries=$enable_retry ? 3 : 0
            ] |->
            UpdateMetrics
            """
        )

        self.assertIn("parallel_workers", program.global_variables)
        self.assertIn("batch_config", program.global_variables)
        self.assertIn("enable_retry", program.global_variables)
        self.assertEqual(program.global_variables["parallel_workers"].value, 5)
        self.assertIsInstance(program.global_variables["batch_config"], MapNode)
        self.assertEqual(
            program.global_variables["batch_config"].value["size"].value, 100
        )
        self.assertEqual(
            program.global_variables["batch_config"].value["timeout"].value, 30
        )
        self.assertEqual(program.global_variables["enable_retry"].value, True)

        self.assertIsNotNone(program.chain)
        self.assertEqual(program.chain.op, "|->")
        self.assertIsInstance(program.chain.right, TaskNode)
        self.assertEqual(program.chain.right.task, "UpdateMetrics")

        left_chain = program.chain.left
        self.assertIsInstance(left_chain, BinOpNode)
        self.assertEqual(left_chain.op, "|->")
        self.assertIsInstance(left_chain.right, MetaTaskNode)
        self.assertEqual(left_chain.right.mode, "MAP")
        self.assertEqual(left_chain.right.template_task, "ProcessWorkItem")
        self.assertEqual(left_chain.right.template_event_namespace, "local")
        self.assertIsInstance(left_chain.right.options, list)
        options = {opt.attr: opt.value for opt in left_chain.right.options}
        self.assertIn("batch_size", options)
        self.assertIn("concurrent", options)
        self.assertIn("retries", options)
        self.assertIsInstance(options["batch_size"], IndexExprNode)
        self.assertEqual(options["concurrent"].value, True)
        self.assertIsInstance(options["retries"], TernaryExprNode)
        self.assertIsInstance(options["retries"].condition, VariableAccessNode)
        self.assertEqual(options["retries"].true_expr.value, 3)
        self.assertEqual(options["retries"].false_expr.value, 0)

        self.assertIsInstance(left_chain.left, BinOpNode)
        self.assertEqual(left_chain.left.op, "->")
        self.assertIsInstance(left_chain.left.left, TaskNode)
        self.assertEqual(left_chain.left.left.task, "ConfigureSystem")
        self.assertIsInstance(left_chain.left.left.options, list)
        self.assertEqual(len(left_chain.left.left.options), 1)
        option = left_chain.left.left.options[0]
        self.assertEqual(option.attr, "worker_count")
        self.assertIsInstance(option.value, VariableAccessNode)
        self.assertEqual(option.value.name, "parallel_workers")
        self.assertIsInstance(left_chain.left.right, TaskNode)
        self.assertEqual(left_chain.left.right.task, "FetchWorkQueue")

    def test_for_each_for_side_effects(self):
        program = pointy_parser(
            """
            @notification_list = [
                {"user": "alice", "message": "..."},
                {"user": "bob", "message": "..."}
            ]
            
            PrepareNotifications |->
            FOREACH<SendEmail>[concurrent=true, continue_on_error=true] |->
            LogCompletionStatus  # Original notification_list is passed through
            """
        )

        self.assertIn("notification_list", program.global_variables)
        self.assertIsInstance(program.global_variables["notification_list"], ListNode)
        self.assertEqual(len(program.global_variables["notification_list"]), 2)
        self.assertIsInstance(program.global_variables["notification_list"][0], MapNode)
        self.assertEqual(
            program.global_variables["notification_list"][0].value["user"].value,
            "alice",
        )
        self.assertEqual(
            program.global_variables["notification_list"][0].value["message"].value,
            "...",
        )
        self.assertIsInstance(program.global_variables["notification_list"][1], MapNode)
        self.assertEqual(
            program.global_variables["notification_list"][1].value["user"].value, "bob"
        )
        self.assertEqual(
            program.global_variables["notification_list"][1].value["message"].value,
            "...",
        )

        self.assertIsNotNone(program.chain)
        self.assertEqual(program.chain.op, "|->")
        self.assertIsInstance(program.chain.right, TaskNode)
        self.assertEqual(program.chain.right.task, "LogCompletionStatus")
        left_chain = program.chain.left
        self.assertIsInstance(left_chain, BinOpNode)
        self.assertEqual(left_chain.op, "|->")
        self.assertIsInstance(left_chain.right, MetaTaskNode)
        self.assertEqual(left_chain.right.mode, "FOREACH")
        self.assertEqual(left_chain.right.template_task, "SendEmail")
        self.assertIsInstance(left_chain.right.options, list)
        options = {opt.attr: opt.value for opt in left_chain.right.options}
        self.assertIn("concurrent", options)
        self.assertIn("continue_on_error", options)
        self.assertEqual(options["concurrent"].value, True)
        self.assertEqual(options["continue_on_error"].value, True)
        self.assertIsInstance(left_chain.left, TaskNode)
        self.assertEqual(left_chain.left.task, "PrepareNotifications")

    def test_error_handling_with_meta_events(self):
        program = pointy_parser(
            """
                @critical_operations = [1, 2, 3]
                
                LoadOperations |->
                MAP<ExecuteCriticalOperation>[retries=5, concurrent=false] (
                    0 |-> FOREACH<LogFailure>[continue_on_error=true] |->
                          NotifyOnCallEngineer |->
                          InitiateRollback,
                    1 -> ValidateAllSuccess -> UpdateStatus
                )
            """
        )

        self.assertIn("critical_operations", program.global_variables)
        self.assertIsInstance(program.global_variables["critical_operations"], ListNode)
        self.assertEqual(len(program.global_variables["critical_operations"]), 3)
        self.assertEqual(program.global_variables["critical_operations"][0].value, 1)
        self.assertEqual(program.global_variables["critical_operations"][1].value, 2)
        self.assertEqual(program.global_variables["critical_operations"][2].value, 3)

        self.assertIsNotNone(program.chain)
        self.assertEqual(program.chain.op, "|->")
        self.assertIsInstance(program.chain.right, ConditionalNode)
        conditional = program.chain.right
        self.assertIsInstance(conditional.task, MetaTaskNode)
        self.assertEqual(conditional.task.mode, "MAP")
        self.assertEqual(conditional.task.template_task, "ExecuteCriticalOperation")
        options = {opt.attr: opt.value for opt in conditional.task.options}
        self.assertIn("retries", options)
        self.assertIn("concurrent", options)
        self.assertEqual(options["retries"].value, 5)
        self.assertEqual(options["concurrent"].value, False)
        # self.assertIsInstance(conditional.branches, BlockNode)
        branches = {a.condition.value: a for a in conditional.branches}
        self.assertIn(0, branches)
        self.assertIn(1, branches)
        # Branch 0
        self.assertEqual(branches[0].operator, "|->")
        self.assertIsInstance(branches[0].task, BinOpNode)
        self.assertEqual(branches[0].task.op, "|->")
        self.assertIsInstance(branches[0].task.right, TaskNode)
        self.assertEqual(branches[0].task.right.task, "InitiateRollback")
        self.assertIsInstance(branches[0].task.left, BinOpNode)
        self.assertEqual(branches[0].task.left.op, "|->")
        self.assertIsInstance(branches[0].task.left.left, MetaTaskNode)
        self.assertEqual(branches[0].task.left.left.mode, "FOREACH")
        self.assertEqual(branches[0].task.left.left.template_task, "LogFailure")
        options = {opt.attr: opt.value for opt in branches[0].task.left.left.options}
        self.assertIn("continue_on_error", options)
        self.assertEqual(options["continue_on_error"].value, True)
        self.assertIsInstance(branches[0].task.left.right, TaskNode)
        self.assertEqual(branches[0].task.left.right.task, "NotifyOnCallEngineer")

        # Branch 1
        self.assertEqual(branches[1].operator, "->")
        self.assertIsInstance(branches[1].task, BinOpNode)
        self.assertEqual(branches[1].task.op, "->")
        self.assertIsInstance(branches[1].task.left, TaskNode)
        self.assertEqual(branches[1].task.left.task, "ValidateAllSuccess")
        self.assertIsInstance(branches[1].task.right, TaskNode)
        self.assertEqual(branches[1].task.right.task, "UpdateStatus")

    def test_explicit_composition(self):
        program = pointy_parser(
            """
                # Process groups of items using flattened composition
                @grouped_data = [
                    {"group": "A", "items": [1, 2, 3]},
                    {"group": "B", "items": [4, 5, 6]}
                ]
                
                LoadGroupedData |->
                FLATMAP<ExtractItems>[concurrent=true] |->  # Flatten all groups to items
                MAP<ProcessItem>[concurrent=true] |->        # Process all items
                REDUCE<AggregateByGroup> |->                 # Re-group results
                GenerateReport
            """
        )

        self.assertIn("grouped_data", program.global_variables)
        self.assertIsInstance(program.global_variables["grouped_data"], ListNode)
        self.assertEqual(len(program.global_variables["grouped_data"]), 2)
        self.assertIsInstance(program.global_variables["grouped_data"][0], MapNode)
        self.assertEqual(
            program.global_variables["grouped_data"][0].value["group"].value, "A"
        )
        self.assertIsInstance(
            program.global_variables["grouped_data"][0].value["items"], ListNode
        )
        self.assertEqual(
            program.global_variables["grouped_data"][0].value["items"][0].value, 1
        )
        self.assertEqual(
            program.global_variables["grouped_data"][0].value["items"][1].value, 2
        )
        self.assertEqual(
            program.global_variables["grouped_data"][0].value["items"][2].value, 3
        )
        self.assertIsInstance(program.global_variables["grouped_data"][1], MapNode)
        self.assertEqual(
            program.global_variables["grouped_data"][1].value["group"].value, "B"
        )
        self.assertIsInstance(
            program.global_variables["grouped_data"][1].value["items"], ListNode
        )
        self.assertEqual(
            program.global_variables["grouped_data"][1].value["items"][0].value, 4
        )
        self.assertEqual(
            program.global_variables["grouped_data"][1].value["items"][1].value, 5
        )
        self.assertEqual(
            program.global_variables["grouped_data"][1].value["items"][2].value, 6
        )

        self.assertIsNotNone(program.chain)
        self.assertEqual(program.chain.op, "|->")
        self.assertIsInstance(program.chain.right, TaskNode)
        self.assertEqual(program.chain.right.task, "GenerateReport")
        left_chain = program.chain.left
        self.assertIsInstance(left_chain, BinOpNode)
        self.assertEqual(left_chain.op, "|->")
        self.assertIsInstance(left_chain.right, MetaTaskNode)
        self.assertEqual(left_chain.right.mode, "REDUCE")
        self.assertEqual(left_chain.right.template_task, "AggregateByGroup")
        self.assertIsInstance(left_chain.left, BinOpNode)
        self.assertEqual(left_chain.left.op, "|->")
        self.assertIsInstance(left_chain.left.right, MetaTaskNode)
        self.assertEqual(left_chain.left.right.mode, "MAP")
        self.assertEqual(left_chain.left.right.template_task, "ProcessItem")
        self.assertIsInstance(left_chain.left.left, BinOpNode)
        self.assertEqual(left_chain.left.left.op, "|->")
        self.assertIsInstance(left_chain.left.left.right, MetaTaskNode)
        self.assertEqual(left_chain.left.left.right.mode, "FLATMAP")
        self.assertEqual(left_chain.left.left.right.template_task, "ExtractItems")
        self.assertIsInstance(left_chain.left.left.left, TaskNode)
        self.assertEqual(left_chain.left.left.left.task, "LoadGroupedData")

    def test_parallel_processing_with_different_meta_event(self):
        program = pointy_parser(
            """
        # Process different data types in parallel
        @data_batch = {"type_a": [], "type_b": [], "type_c": []}
    
        SplitDataByType -> (
            ExtractTypeA |-> MAP<ProcessTypeA>[concurrent=true] ||
            ExtractTypeB |-> MAP<ProcessTypeB>[concurrent=true] ||
            ExtractTypeC |-> FLATMAP<ProcessTypeC>[concurrent=true]
        ) |-> MergeResults |-> FinalValidation
        """
        )

        self.assertIn("data_batch", program.global_variables)
        self.assertIsInstance(program.global_variables["data_batch"], MapNode)
        self.assertIn("type_a", program.global_variables["data_batch"].value)
        self.assertIn("type_b", program.global_variables["data_batch"].value)
        self.assertIn("type_c", program.global_variables["data_batch"].value)

        self.assertIsNotNone(program.chain)
        self.assertEqual(program.chain.op, "|->")
        self.assertIsInstance(program.chain.right, TaskNode)
        self.assertEqual(program.chain.right.task, "FinalValidation")
        left_chain = program.chain.left
        self.assertIsInstance(left_chain, BinOpNode)
        self.assertEqual(left_chain.op, "|->")
        self.assertIsInstance(left_chain.right, TaskNode)
        self.assertEqual(left_chain.right.task, "MergeResults")
        self.assertIsInstance(left_chain.left, BinOpNode)
        self.assertEqual(left_chain.left.op, "->")

        block = left_chain.left.right
        self.assertIsInstance(block, BinOpNode)

        # With equal precedence and left-associative parsing the parenthesized
        # expression folds left-to-right. Final top op becomes '|->'.
        self.assertEqual(block.op, "|->")

        # Rightmost operation applies FLATMAP to the accumulated left expression
        self.assertIsInstance(block.right, MetaTaskNode)
        self.assertEqual(block.right.mode, "FLATMAP")
        self.assertEqual(block.right.template_task, "ProcessTypeC")

        # Left of the final '|->' is a '||' combining previous results with ExtractTypeC
        self.assertIsInstance(block.left, BinOpNode)
        self.assertEqual(block.left.op, "||")
        self.assertIsInstance(block.left.right, TaskNode)
        self.assertEqual(block.left.right.task, "ExtractTypeC")

        # The left side of that '||' is itself a '|->' where MAP<ProcessTypeB> was applied
        self.assertIsInstance(block.left.left, BinOpNode)
        self.assertEqual(block.left.left.op, "|->")
        self.assertIsInstance(block.left.left.right, MetaTaskNode)
        self.assertEqual(block.left.left.right.mode, "MAP")
        self.assertEqual(block.left.left.right.template_task, "ProcessTypeB")

        # That '|->' has a left child which is a '||' combining the A branch and ExtractTypeB
        self.assertIsInstance(block.left.left.left, BinOpNode)
        self.assertEqual(block.left.left.left.op, "||")

        # Left part of that '||' is the original A branch: ExtractTypeA |-> MAP<ProcessTypeA>
        self.assertIsInstance(block.left.left.left.left, BinOpNode)
        self.assertEqual(block.left.left.left.left.op, "|->")
        self.assertIsInstance(block.left.left.left.left.left, TaskNode)
        self.assertEqual(block.left.left.left.left.left.task, "ExtractTypeA")
        self.assertIsInstance(block.left.left.left.left.right, MetaTaskNode)
        self.assertEqual(block.left.left.left.left.right.mode, "MAP")
        self.assertEqual(block.left.left.left.left.right.template_task, "ProcessTypeA")

        # Right part of that inner '||' is ExtractTypeB
        self.assertIsInstance(block.left.left.left.right, TaskNode)
        self.assertEqual(block.left.left.left.right.task, "ExtractTypeB")

    def test_fan_out_to_multiple_services(self):
        program = pointy_parser(
            """
            # Broadcast request to multiple replicas for redundancy
            @request_payload = {"query": "search term", "filters": {"date_range": "last_30_days", "category": "books"}}
            
            PrepareRequest |->
            FANOUT<SendToSearchReplica>[count=3, concurrent=true] |->
            SelectFastestResponse |->
            FormatResults |->
            CacheAndReturn
            """
        )

        self.assertIn("request_payload", program.global_variables)
        self.assertIsInstance(program.global_variables["request_payload"], MapNode)
        self.assertIn("query", program.global_variables["request_payload"].value)
        self.assertIn("filters", program.global_variables["request_payload"].value)

        self.assertIsNotNone(program.chain)
        self.assertEqual(program.chain.op, "|->")
        self.assertIsInstance(program.chain.right, TaskNode)
        self.assertEqual(program.chain.right.task, "CacheAndReturn")
        left_chain = program.chain.left
        self.assertIsInstance(left_chain, BinOpNode)
        self.assertEqual(left_chain.op, "|->")
        self.assertIsInstance(left_chain.right, TaskNode)
        self.assertEqual(left_chain.right.task, "FormatResults")
        self.assertIsInstance(left_chain.left, BinOpNode)
        self.assertEqual(left_chain.left.op, "|->")
        self.assertIsInstance(left_chain.left.right, TaskNode)
        self.assertEqual(left_chain.left.right.task, "SelectFastestResponse")
        self.assertIsInstance(left_chain.left.left, BinOpNode)
        self.assertEqual(left_chain.left.left.op, "|->")
        self.assertIsInstance(left_chain.left.left.right, MetaTaskNode)
        self.assertEqual(left_chain.left.left.right.mode, "FANOUT")
        self.assertEqual(
            left_chain.left.left.right.template_task, "SendToSearchReplica"
        )
        options = {opt.attr: opt.value for opt in left_chain.left.left.right.options}
        self.assertIn("count", options)
        self.assertIn("concurrent", options)
        self.assertEqual(options["count"].value, 3)
        self.assertEqual(options["concurrent"].value, True)
        self.assertIsInstance(left_chain.left.left.left, TaskNode)
        self.assertEqual(left_chain.left.left.left.task, "PrepareRequest")

    def test_example_complex_multi_stage_pipeline(self):
        program = pointy_parser(
            """
            @batch_size = 50
            @retry_count = 3
            
            # Extract, transform, load pipeline
            FetchRawData |->
            MAP<ParseRecord>[batch_size=$batch_size, concurrent=true, retries=$retry_count] (
                0 -> LogParseErrors -> NotifyDataTeam,
                1 -> FILTER<ValidateSchema>[concurrent=true] |->
                     MAP<TransformToTargetFormat>[concurrent=true] |->
                     REDUCE<BatchInsert>[batch_size=100] (
                         0 -> RollbackChanges -> AlertAdmin,
                         1 -> CommitTransaction -> SendSuccessNotification
                     )
            )
            """
        )

        self.assertIn("batch_size", program.global_variables)
        self.assertIn("retry_count", program.global_variables)
        self.assertEqual(program.global_variables["batch_size"].value, 50)
        self.assertEqual(program.global_variables["retry_count"].value, 3)

        self.assertIsNotNone(program.chain)
        self.assertEqual(program.chain.op, "|->")
        self.assertIsInstance(program.chain.left, TaskNode)
        self.assertEqual(program.chain.left.task, "FetchRawData")
        self.assertIsInstance(program.chain.right, ConditionalNode)
        conditional = program.chain.right
        self.assertIsInstance(conditional.task, MetaTaskNode)
        self.assertEqual(conditional.task.mode, "MAP")
        self.assertEqual(conditional.task.template_task, "ParseRecord")
        options = {opt.attr: opt.value for opt in conditional.task.options}
        self.assertIn("batch_size", options)
        self.assertIn("concurrent", options)
        self.assertIn("retries", options)
        self.assertIsInstance(options["batch_size"], VariableAccessNode)
        self.assertEqual(options["batch_size"].value.value, 50)
        self.assertEqual(options["concurrent"].value, True)
        self.assertIsInstance(options["retries"], VariableAccessNode)
        self.assertEqual(options["retries"].name, "retry_count")
        # self.assertIsInstance(conditional.branches, BlockNode)
        branches = {a.condition.value: a for a in conditional.branches}
        self.assertIn(0, branches)
        self.assertIn(1, branches)
        # Branch 0
        self.assertEqual(branches[0].operator, "->")
        self.assertIsInstance(branches[0].task, BinOpNode)
        self.assertEqual(branches[0].task.op, "->")
        self.assertIsInstance(branches[0].task.left, TaskNode)
        self.assertEqual(branches[0].task.left.task, "LogParseErrors")
        self.assertIsInstance(branches[0].task.right, TaskNode)
        self.assertEqual(branches[0].task.right.task, "NotifyDataTeam")
        # Branch 1
        self.assertEqual(branches[1].operator, "->")
        self.assertIsInstance(branches[1].task, BinOpNode)
        self.assertEqual(branches[1].task.op, "|->")
        self.assertIsInstance(branches[1].task.left, BinOpNode)
        self.assertEqual(branches[1].task.left.op, "|->")
        self.assertIsInstance(branches[1].task.left.left, MetaTaskNode)
        self.assertEqual(branches[1].task.left.left.mode, "FILTER")
        self.assertEqual(branches[1].task.left.left.template_task, "ValidateSchema")
        options = {opt.attr: opt.value for opt in branches[1].task.left.left.options}
        self.assertIn("concurrent", options)
        self.assertEqual(options["concurrent"].value, True)
        self.assertIsInstance(branches[1].task.left.right, MetaTaskNode)
        self.assertEqual(branches[1].task.left.right.mode, "MAP")
        self.assertEqual(
            branches[1].task.left.right.template_task, "TransformToTargetFormat"
        )
        options = {opt.attr: opt.value for opt in branches[1].task.left.right.options}
        self.assertIn("concurrent", options)
        self.assertEqual(options["concurrent"].value, True)

        self.assertIsInstance(branches[1].task.right, ConditionalNode)
        self.assertIsInstance(branches[1].task.right.task, MetaTaskNode)
        self.assertEqual(branches[1].task.right.task.mode, "REDUCE")
        self.assertEqual(branches[1].task.right.task.template_task, "BatchInsert")
        options = {opt.attr: opt.value for opt in branches[1].task.right.task.options}
        self.assertIn("batch_size", options)
        self.assertEqual(options["batch_size"].value, 100)
        # self.assertIsInstance(branches[1].task.right.branches, BlockNode)
        reduce_branches = {
            a.condition.value: a for a in branches[1].task.right.branches
        }
        self.assertIn(0, reduce_branches)
        self.assertIn(1, reduce_branches)
        # Reduce Branch 0
        self.assertEqual(reduce_branches[0].operator, "->")
        self.assertIsInstance(reduce_branches[0].task, BinOpNode)
        self.assertEqual(reduce_branches[0].task.op, "->")
        self.assertIsInstance(reduce_branches[0].task.left, TaskNode)
        self.assertEqual(reduce_branches[0].task.left.task, "RollbackChanges")
        self.assertIsInstance(reduce_branches[0].task.right, TaskNode)
        self.assertEqual(reduce_branches[0].task.right.task, "AlertAdmin")
        # Reduce Branch 1
        self.assertEqual(reduce_branches[1].operator, "->")
        self.assertIsInstance(reduce_branches[1].task, BinOpNode)
        self.assertEqual(reduce_branches[1].task.op, "->")
        self.assertIsInstance(reduce_branches[1].task.left, TaskNode)
        self.assertEqual(reduce_branches[1].task.left.task, "CommitTransaction")
        self.assertIsInstance(reduce_branches[1].task.right, TaskNode)
        self.assertEqual(reduce_branches[1].task.right.task, "SendSuccessNotification")

    def test_example_data_aggregation_pipeline(self):
        program = pointy_parser(
            """
            # Aggregate sales data
            @sales_records = [
                {"amount": 100, "region": "North"},
                {"amount": 200, "region": "South"},
                {"amount": 150, "region": "North"}
            ]
            
            LoadSalesData |->
            MAP<ValidateRecord>[concurrent=true] |->
            FILTER<IsValidSale> |->
            REDUCE<SumAmounts>[initial_value=0] |->
            PublishTotalSales
            """
        )

        self.assertIn("sales_records", program.global_variables)
        self.assertIsInstance(program.global_variables["sales_records"], ListNode)
        self.assertEqual(len(program.global_variables["sales_records"]), 3)
        self.assertIsInstance(program.global_variables["sales_records"][0], MapNode)
        self.assertEqual(
            program.global_variables["sales_records"][0].value["amount"].value, 100
        )
        self.assertEqual(
            program.global_variables["sales_records"][0].value["region"].value, "North"
        )
        self.assertIsInstance(program.global_variables["sales_records"][1], MapNode)
        self.assertEqual(
            program.global_variables["sales_records"][1].value["amount"].value, 200
        )
        self.assertEqual(
            program.global_variables["sales_records"][1].value["region"].value, "South"
        )
        self.assertIsInstance(program.global_variables["sales_records"][2], MapNode)
        self.assertEqual(
            program.global_variables["sales_records"][2].value["amount"].value, 150
        )
        self.assertEqual(
            program.global_variables["sales_records"][2].value["region"].value, "North"
        )

        self.assertIsNotNone(program.chain)
        self.assertEqual(program.chain.op, "|->")
        self.assertIsInstance(program.chain.right, TaskNode)
        self.assertEqual(program.chain.right.task, "PublishTotalSales")
        left_chain = program.chain.left
        self.assertIsInstance(left_chain, BinOpNode)
        self.assertEqual(left_chain.op, "|->")
        self.assertIsInstance(left_chain.right, MetaTaskNode)
        self.assertEqual(left_chain.right.mode, "REDUCE")
        self.assertEqual(left_chain.right.template_task, "SumAmounts")
        options = {opt.attr: opt.value for opt in left_chain.right.options}
        self.assertIn("initial_value", options)
        self.assertEqual(options["initial_value"].value, 0)
        self.assertIsInstance(left_chain.left, BinOpNode)
        self.assertEqual(left_chain.left.op, "|->")
        self.assertIsInstance(left_chain.left.right, MetaTaskNode)
        self.assertEqual(left_chain.left.right.mode, "FILTER")
        self.assertEqual(left_chain.left.right.template_task, "IsValidSale")
        self.assertIsInstance(left_chain.left.left, BinOpNode)
        self.assertEqual(left_chain.left.left.op, "|->")
        self.assertIsInstance(left_chain.left.left.right, MetaTaskNode)
        self.assertEqual(left_chain.left.left.right.mode, "MAP")
        self.assertEqual(left_chain.left.left.right.template_task, "ValidateRecord")
        self.assertIsInstance(left_chain.left.left.left, TaskNode)
        self.assertEqual(left_chain.left.left.left.task, "LoadSalesData")

    def test_example_filter_and_process(self):
        program = pointy_parser(
            """
            # Filter adult users and process
            @users = [
                {"name": "Alice", "age": 25},
                {"name": "Bob", "age": 17},
                {"name": "Charlie", "age": 30}
            ]
            
            LoadUsers |-> 
            FILTER<IsAdult>[concurrent=true] |-> 
            MAP<SendMarketingEmail>[batch_size=10] |->
            LogResults
            """
        )

        self.assertIn("users", program.global_variables)
        self.assertIsInstance(program.global_variables["users"], ListNode)
        self.assertEqual(len(program.global_variables["users"]), 3)
        self.assertIsInstance(program.global_variables["users"][0], MapNode)
        self.assertEqual(
            program.global_variables["users"][0].value["name"].value, "Alice"
        )
        self.assertEqual(program.global_variables["users"][0].value["age"].value, 25)
        self.assertIsInstance(program.global_variables["users"][1], MapNode)
        self.assertEqual(
            program.global_variables["users"][1].value["name"].value, "Bob"
        )
        self.assertEqual(program.global_variables["users"][1].value["age"].value, 17)
        self.assertIsInstance(program.global_variables["users"][2], MapNode)
        self.assertEqual(
            program.global_variables["users"][2].value["name"].value, "Charlie"
        )
        self.assertEqual(program.global_variables["users"][2].value["age"].value, 30)

        self.assertIsNotNone(program.chain)
        self.assertEqual(program.chain.op, "|->")
        self.assertIsInstance(program.chain.right, TaskNode)
        self.assertEqual(program.chain.right.task, "LogResults")
        left_chain = program.chain.left
        self.assertIsInstance(left_chain, BinOpNode)
        self.assertEqual(left_chain.op, "|->")
        self.assertIsInstance(left_chain.right, MetaTaskNode)
        self.assertEqual(left_chain.right.mode, "MAP")
        self.assertEqual(left_chain.right.template_task, "SendMarketingEmail")
        options = {opt.attr: opt.value for opt in left_chain.right.options}
        self.assertIn("batch_size", options)
        self.assertEqual(options["batch_size"].value, 10)
        self.assertIsInstance(left_chain.left, BinOpNode)
        self.assertEqual(left_chain.left.op, "|->")
        self.assertIsInstance(left_chain.left.right, MetaTaskNode)
        self.assertEqual(left_chain.left.right.mode, "FILTER")
        self.assertEqual(left_chain.left.right.template_task, "IsAdult")
        options = {opt.attr: opt.value for opt in left_chain.left.right.options}
        self.assertIn("concurrent", options)
        self.assertEqual(options["concurrent"].value, True)
        self.assertIsInstance(left_chain.left.left, TaskNode)
        self.assertEqual(left_chain.left.left.task, "LoadUsers")

    def test_example_simple_map_operation(self):
        program = pointy_parser(
            """
            # Process a list of user IDs
            @user_ids = [101, 102, 103, 104, 105]
            
            FetchUserIds |-> MAP<EnrichUserData>[concurrent=true, retries=2] |-> SaveToCache
            """
        )

        self.assertIn("user_ids", program.global_variables)
        self.assertIsInstance(program.global_variables["user_ids"], ListNode)
        self.assertEqual(len(program.global_variables["user_ids"]), 5)
        self.assertEqual(program.global_variables["user_ids"][0].value, 101)
        self.assertEqual(program.global_variables["user_ids"][1].value, 102)
        self.assertEqual(program.global_variables["user_ids"][2].value, 103)
        self.assertEqual(program.global_variables["user_ids"][3].value, 104)
        self.assertEqual(program.global_variables["user_ids"][4].value, 105)

        self.assertIsNotNone(program.chain)
        self.assertEqual(program.chain.op, "|->")
        self.assertIsInstance(program.chain.right, TaskNode)
        self.assertEqual(program.chain.right.task, "SaveToCache")
        left_chain = program.chain.left
        self.assertIsInstance(left_chain, BinOpNode)
        self.assertEqual(left_chain.op, "|->")
        self.assertIsInstance(left_chain.right, MetaTaskNode)
        self.assertEqual(left_chain.right.mode, "MAP")
        self.assertEqual(left_chain.right.template_task, "EnrichUserData")
        options = {opt.attr: opt.value for opt in left_chain.right.options}
        self.assertIn("concurrent", options)
        self.assertIn("retries", options)
        self.assertEqual(options["concurrent"].value, True)
        self.assertEqual(options["retries"].value, 2)
        self.assertIsInstance(left_chain.left, TaskNode)
        self.assertEqual(left_chain.left.task, "FetchUserIds")


if __name__ == "__main__":
    unittest.main()
