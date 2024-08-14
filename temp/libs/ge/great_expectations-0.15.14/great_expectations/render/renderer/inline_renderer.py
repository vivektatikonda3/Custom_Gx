import logging
from typing import Callable, List, Optional, Tuple, Union

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.expectations.registry import (
    get_renderer_impl,
    get_renderer_names_with_renderer_prefix,
)
from great_expectations.render.renderer.renderer import Renderer
from great_expectations.render.types import RenderedAtomicContent

logger = logging.getLogger(__name__)


class InlineRenderer(Renderer):
    def __init__(
        self,
        render_object: ExpectationValidationResult,
    ) -> None:
        super().__init__()

        self._render_object = render_object

    def get_atomic_rendered_content_for_object(
        self,
        render_object: Union[ExpectationConfiguration, ExpectationValidationResult],
    ) -> List[RenderedAtomicContent]:
        """Gets RenderedAtomicContent for a given ExpectationConfiguration or ExpectationValidationResult.

        Args:
            render_object: The object to render.

        Returns:
            A list of RenderedAtomicContent objects for a given ExpectationConfiguration or ExpectationValidationResult.
        """
        expectation_type: str
        atomic_renderer_prefix: str
        legacy_renderer_prefix: str
        if isinstance(render_object, ExpectationConfiguration):
            expectation_type = render_object.expectation_type
            renderer_prefix = "atomic.prescriptive"
        elif isinstance(render_object, ExpectationValidationResult):
            expectation_type = render_object.expectation_config.expectation_type
            renderer_prefix = "atomic.diagnostic"
        else:
            raise ValueError(
                f"""object must be of type ExpectationConfiguration or ExpectationValidationResult,
but an object of type "{type(render_object)}" was passed."""
            )

        renderer_names: List[str] = get_renderer_names_with_renderer_prefix(
            object_name=expectation_type,
            renderer_prefix=renderer_prefix,
        )

        rendered_content: List[
            RenderedAtomicContent
        ] = self._get_atomic_rendered_content_from_renderer_names(
            render_object=render_object,
            renderer_names=renderer_names,
            expectation_type=expectation_type,
        )

        return rendered_content

    def _get_atomic_rendered_content_from_renderer_names(
        self,
        render_object: Union[ExpectationConfiguration, ExpectationValidationResult],
        renderer_names: List[str],
        expectation_type: str,
    ) -> List[RenderedAtomicContent]:
        default_prescriptive_renderer_name: str = "atomic.prescriptive.kwargs"
        non_default_prescriptive_renderer_names: List[str] = [
            renderer_name
            for renderer_name in renderer_names
            if renderer_name != default_prescriptive_renderer_name
        ]

        renderer_rendered_content: Optional[RenderedAtomicContent]
        rendered_content: List[RenderedAtomicContent] = []
        for renderer_name in non_default_prescriptive_renderer_names:
            try:
                renderer_rendered_content = self._get_renderer_atomic_rendered_content(
                    render_object=render_object,
                    renderer_name=renderer_name,
                    expectation_type=expectation_type,
                )
                rendered_content.append(renderer_rendered_content)
            except Exception:
                logger.info(
                    f'Renderer "{renderer_name}" failed to render Expectation "{expectation_type}".'
                )

        if len(rendered_content) == 0 and isinstance(
            render_object, ExpectationConfiguration
        ):
            logger.info(
                f"""The following renderers failed to render Expectation "{expectation_type}":
{non_default_prescriptive_renderer_names}
Default renderer "{default_prescriptive_renderer_name}" will be used to render prescriptive content for ExpectationConfiguration.
"""
            )
            renderer_rendered_content = (
                InlineRenderer._get_renderer_atomic_rendered_content(
                    render_object=render_object,
                    renderer_name=default_prescriptive_renderer_name,
                    expectation_type=expectation_type,
                )
            )
            rendered_content.append(renderer_rendered_content)

        return rendered_content

    @staticmethod
    def _get_renderer_atomic_rendered_content(
        render_object: Union[ExpectationConfiguration, ExpectationValidationResult],
        renderer_name: str,
        expectation_type: str,
    ) -> Optional[RenderedAtomicContent]:
        renderer_rendered_content: Optional[RenderedAtomicContent] = None
        renderer_tuple: Optional[tuple] = get_renderer_impl(
            object_name=expectation_type, renderer_type=renderer_name
        )
        if renderer_tuple is not None:
            # index 0 is expectation class-name and index 1 is implementation of renderer
            renderer_fn: Callable = renderer_tuple[1]
            if isinstance(render_object, ExpectationConfiguration):
                renderer_rendered_content = renderer_fn(configuration=render_object)
            else:
                renderer_rendered_content = renderer_fn(result=render_object)

        return renderer_rendered_content

    def render(self) -> Tuple[List[RenderedAtomicContent], List[RenderedAtomicContent]]:
        """Gets RenderedAtomicContent for a given ExpectationConfiguration or ExpectationValidationResult.

        Returns:
            A tuple containing RenderedAtomicContent objects for a given ExpectationConfiguration (index 0) and
            ExpectationValidationResult (index 1).
        """
        render_object: ExpectationValidationResult = self._render_object

        return self.get_atomic_rendered_content_for_object(
            render_object=render_object.expectation_config
        ), self.get_atomic_rendered_content_for_object(render_object=render_object)
