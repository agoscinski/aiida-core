from aiida.engine import WorkChain


class SomeWorkChain(WorkChain):
    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.outline(
            cls.submit_workchains,
            cls.inspect_workchains,
        )

    def submit_workchains(self):
        for i in range(3):
            future = self.submit(SomeWorkChain)
            key = f'workchain_{i}'
            self.to_context(**{key: future})

    def inspect_workchains(self):
        for i in range(3):
            key = f'workchain_{i}'
            assert self.ctx[key].is_finished_ok
